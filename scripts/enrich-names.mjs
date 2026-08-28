// Волна имён: то, чего нет у Шикимори, добирается с anime365.
//
// ЗАЧЕМ. В первом выпуске 4 204 записи с пустым полем russian: тайтл Шикимори
// знает, а русского имени у него нет. База anime365 во многом та же, но не
// целиком, и каждое найденное здесь имя — это сетевой запрос, которого потом
// не сделает ни один клиент.
//
// ТОЛЬКО КИРИЛЛИЦА. anime365 нередко кладёт в поле ru латинское написание.
// Такое имя хуже пустоты: клиент примет его за перевод, покажет латиницу
// как русское название и не переспросит уже никогда. Пустое поле честнее.
//
// ВОЛНА НЕОБЯЗАТЕЛЬНАЯ и идёт последней: источник отвечает 403 и пятисотыми
// от Cloudflare пачками, а сборка не вправе от него зависеть. У волны есть
// свой бюджет времени: она обязана уступить место выпуску, а не съесть его.

import { bump, pct, round1, sleep, why } from './common.mjs'

/** Адреса по порядку: те же, что знает клиент. */
const MIRRORS = ['anime365.ru', 'smotret-anime.online']
/** Коды, означающие «источник жив, но сейчас не отдаёт». */
const BLOCKED = [403, 429, 502, 503, 520, 521, 522, 523, 524]
/** Из полной записи нужен ровно один кусок: 229 КБ против двух с половиной. */
const FIELDS = 'titles'
const TIMEOUT_MS = 10000
const PAUSE_MS = Number(process.env.NAMES_PAUSE || 700)
/** После скольких отказов подряд адрес меняется. */
const SWITCH_AFTER = 3
/** После скольких отказов подряд волна сдаётся целиком. */
const GIVE_UP_AFTER = 12
/** Бюджет волны. Раньше кончится он — раньше кончится и волна. */
const BUDGET_MS = Number(process.env.NAMES_BUDGET || 60) * 60000
const REPORT_EVERY = 200
const CYRILLIC = /[А-Яа-яЁё]/
const UA = 'AniMori/3.0 (+https://github.com/foulnike/animori-data)'

/** Один запрос к зеркалу. Отказ не бросается: волна разбирает его сама. */
async function ask(domain, malId) {
  const url = `https://${domain}/api/series?myAnimeListId=${malId}&limit=1&fields=${FIELDS}`
  const started = Date.now()

  try {
    const answer = await fetch(url, {
      headers: { 'user-agent': UA, accept: 'application/json' },
      signal: AbortSignal.timeout(TIMEOUT_MS),
    })
    const took = Date.now() - started

    if (answer.status === 404) return { status: 404, took, name: '' }
    if (answer.status !== 200) {
      const retryAfter = Number(answer.headers.get('retry-after') || 0)
      return { status: answer.status, took, retryAfter }
    }

    const body = await answer.json()
    const item = Array.isArray(body.data) ? body.data[0] : null
    const name = item && item.titles && typeof item.titles.ru === 'string' ? item.titles.ru.trim() : ''
    return { status: 200, took, name }
  } catch (e) {
    return { status: `сеть: ${why(e)}`, took: Date.now() - started }
  }
}

/**
 * Добирает имена для записей с пустым russian.
 * Возвращает карту «номер MAL → русское имя» и статистику волны.
 *
 * @param rows записи обхода Шикимори
 */
export async function enrichNames(rows) {
  const stat = {
    skipped: false,
    empty: 0,
    asked: 0,
    requests: 0,
    added: 0,
    latin: 0,
    codes: {},
    tookMs: 0,
    gaveUp: false,
    mirror: MIRRORS[0],
  }
  const names = new Map()
  const startedAll = Date.now()

  if (process.env.BUILD_ANIME365 === 'off') {
    console.log('Имена: волна anime365 выключена настройкой')
    stat.skipped = true
    return { names, stat }
  }

  const empty = rows.filter((row) => row.russian === '').map((row) => row.id)
  stat.empty = empty.length

  console.log(`Имена: без русского названия ${empty.length}, бюджет ${round1(BUDGET_MS / 60000)} мин`)
  if (empty.length === 0) return { names, stat }

  let mirrorAt = 0
  let misses = 0

  for (const malId of empty) {
    if (Date.now() - startedAll > BUDGET_MS) {
      stat.gaveUp = true
      console.log(`Имена: бюджет времени исчерпан, волна свёрнута на ${stat.asked} из ${empty.length}`)
      break
    }

    const domain = MIRRORS[mirrorAt]
    const answer = await ask(domain, malId)

    stat.requests++
    bump(stat.codes, answer.status)

    if (answer.status !== 200 && answer.status !== 404) {
      misses++

      // Лимит просит подождать, а не уйти: пауза длиннее обычной.
      if (answer.status === 429) await sleep(Math.max(answer.retryAfter * 1000, 5000))

      if (misses >= GIVE_UP_AFTER) {
        stat.gaveUp = true
        console.log(`Имена: ${GIVE_UP_AFTER} отказов подряд, волна свёрнута`)
        break
      }

      if (misses % SWITCH_AFTER === 0) {
        mirrorAt = (mirrorAt + 1) % MIRRORS.length
        stat.mirror = MIRRORS[mirrorAt]
        console.log(`Имена: уходим на ${stat.mirror}, ${domain} отвечает ${answer.status}`)
      }

      await sleep(PAUSE_MS)
      continue
    }

    misses = 0
    stat.asked++

    const name = answer.name || ''
    if (name !== '') {
      if (CYRILLIC.test(name)) {
        names.set(malId, name)
        stat.added++
      } else {
        // Латиница в поле ru: у Шикимори такого имени нет, и здесь его тоже нет.
        stat.latin++
      }
    }

    if (stat.asked % REPORT_EVERY === 0) {
      console.log(`Имена: ${stat.asked} из ${empty.length} (${pct(stat.asked / empty.length)}), нашли ${stat.added}`)
    }

    await sleep(PAUSE_MS)
  }

  stat.tookMs = Date.now() - startedAll
  console.log(`Имена: добавлено ${stat.added} за ${round1(stat.tookMs / 60000)} мин`)

  return { names, stat }
}
