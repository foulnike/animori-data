// Волна имён: то, чего нет у Шикимори, добирается с anime365.
//
// ЗАЧЕМ. В первом выпуске 4 204 записи с пустым полем russian: тайтл Шикимори
// знает, а русского имени у него нет. База anime365 во многом та же, но не
// целиком, и каждое найденное здесь имя — это сетевой запрос, которого потом
// не сделает ни один клиент.
//
// ЖУРНАЛ УЖЕ СПРОШЕННОГО. Список пустых от недели к неделе почти тот же,
// и без памяти волна каждый раз заново спрашивала про те же четыре тысячи
// тайтлов ради трёх десятков новых имён. Ответ «русского имени нет» теперь
// записывается в JOURNAL и повторяется не чаще раза в RETRY_DAYS: база anime365
// меняется, но медленно, а бюджет волны лучше потратить на новинки каталога.
// Журнал лежит в репозитории рядом с .github/last-build и ездит тем же коммитом:
// в выпуске ему не место — клиентам он ни к чему, это черновик сборщика.
//
// ТОЛЬКО КИРИЛЛИЦА. anime365 нередко кладёт в поле ru латинское написание.
// Такое имя хуже пустоты: клиент примет его за перевод, покажет латиницу
// как русское название и не переспросит уже никогда. Пустое поле честнее.
//
// ВОЛНА НЕОБЯЗАТЕЛЬНАЯ и идёт последней: источник отвечает 403 и пятисотыми
// от Cloudflare пачками, а сборка не вправе от него зависеть. У волны есть
// свой бюджет времени: она обязана уступить место выпуску, а не съесть его.

import { readFile, writeFile, mkdir } from 'node:fs/promises'
import { dirname } from 'node:path'

import { bump, pct, round1, sleep, why } from './common.mjs'

/** Адреса по порядку: те же, что знает клиент. */
const MIRRORS = ['anime365.ru', 'smotret-anime.online']
/** Из полной записи нужен ровно один кусок: 229 КБ против двух с половиной. */
const FIELDS = 'titles'
const TIMEOUT_MS = 10000
const PAUSE_MS = Number(process.env.NAMES_PAUSE || 700)
/** После скольких отказов подряд адрес меняется. */
const SWITCH_AFTER = 3
/** После скольких отказов подряд волна сдаётся целиком. */
const GIVE_UP_AFTER = 12
/** Бюджет волны в минутах. Раньше кончится он — раньше кончится и волна. */
const BUDGET_MS = Number(process.env.NAMES_BUDGET || 60) * 60000
const REPORT_EVERY = 200
const CYRILLIC = /[А-Яа-яЁё]/
const UA = 'AniMori/3.0 (+https://github.com/foulnike/animori-data)'

/** Где лежит журнал уже спрошенного. Файл репозитория, а не выпуска. */
const JOURNAL = '.github/anime365-empty.json'
/** Через сколько дней пустой ответ стоит переспросить. */
const RETRY_DAYS = Number(process.env.NAMES_RETRY_DAYS || 180)
const DAY_MS = 86400000

/** Сегодняшнее число в виде ГГГГ-ММ-ДД: журнал читается и глазами. */
function today() {
  return new Date().toISOString().slice(0, 10)
}

/**
 * Журнал с диска. Отсутствие файла и любая его порча — не беда: журнал
 * всего лишь экономит запросы, и без него волна просто работает как раньше.
 */
async function readJournal() {
  try {
    const raw = JSON.parse(await readFile(JOURNAL, 'utf8'))
    const asked = raw && typeof raw.asked === 'object' && raw.asked !== null ? raw.asked : {}
    return asked
  } catch {
    return {}
  }
}

/** Кладёт журнал обратно. Отказ записи тоже не беда и выпуск не срывает. */
async function writeJournal(asked) {
  const ids = Object.keys(asked).sort((a, b) => Number(a) - Number(b))
  const sorted = {}
  for (const id of ids) sorted[id] = asked[id]

  try {
    await mkdir(dirname(JOURNAL), { recursive: true })
    await writeFile(
      JOURNAL,
      JSON.stringify({ note: 'Номера MAL, у которых anime365 не знает русского имени', asked: sorted }) + '\n',
    )
    console.log(`Имена: в журнале ${ids.length} номеров без имени`)
  } catch (e) {
    console.log(`Имена: журнал не записался (${why(e)})`)
  }
}

/** Один запрос к зеркалу. Отказ не бросается: волна разбирает его сама. */
async function ask(domain, malId) {
  const url =
    'https://' + domain + `/api/series?myAnimeListId=${malId}&limit=1&fields=${FIELDS}`
  const started = Date.now()

  try {
    const answer = await fetch(url, {
      headers: { 'user-agent': UA, accept: 'application/json' },
      signal: AbortSignal.timeout(TIMEOUT_MS),
    })
    const took = Date.now() - started

    // 404 — источник ответил, данных просто нет. Это не отказ адреса.
    if (answer.status === 404) return { status: 404, took, name: '' }

    if (answer.status !== 200) {
      const retryAfter = Number(answer.headers.get('retry-after') || 0)
      return { status: answer.status, took, retryAfter }
    }

    const body = await answer.json()
    const item = Array.isArray(body.data) ? body.data[0] : null
    const ru = item && item.titles ? item.titles.ru : null
    return { status: 200, took, name: typeof ru === 'string' ? ru.trim() : '' }
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
    known: 0,
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

  const journal = await readJournal()
  const stale = Date.now() - RETRY_DAYS * DAY_MS

  const blank = rows.filter((row) => row.russian === '').map((row) => row.id)
  stat.empty = blank.length

  // Спрашиваем только тех, про кого либо не спрашивали вовсе, либо
  // спрашивали давно. Новинки каталога идут первыми и сами собой:
  // бюджета может не хватить на всех, а у нового тайтла шанс выше.
  const empty = []
  for (const malId of blank) {
    const seenAt = journal[String(malId)]
    const at = typeof seenAt === 'string' ? Date.parse(seenAt) : NaN
    if (Number.isFinite(at) && at > stale) {
      stat.known++
      continue
    }
    empty.push(malId)
  }
  empty.sort((a, b) => b - a)

  console.log(
    `Имена: без русского названия ${blank.length}, из них спрашивали недавно ${stat.known}, ` +
      `спросим ${empty.length}, бюджет ${round1(BUDGET_MS / 60000)} мин`,
  )
  if (empty.length === 0) return { names, stat }

  let mirrorAt = 0
  let misses = 0
  const stamp = today()

  for (const malId of empty) {
    if (Date.now() - startedAll > BUDGET_MS) {
      stat.gaveUp = true
      console.log(
        `Имена: бюджет исчерпан, свёрнуто на ${stat.asked} из ${empty.length}`,
      )
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
    if (name !== '' && CYRILLIC.test(name)) {
      names.set(malId, name)
      stat.added++
      // Имя нашлось: строка журнала больше не нужна и только занимала бы место.
      delete journal[String(malId)]
    } else {
      // Пусто или латиница — для нас одно и то же: русского имени здесь нет.
      if (name !== '') stat.latin++
      journal[String(malId)] = stamp
    }

    if (stat.asked % REPORT_EVERY === 0) {
      console.log(
        `Имена: ${stat.asked} из ${empty.length} (${pct(stat.asked / empty.length)}), нашли ${stat.added}`,
      )
    }

    await sleep(PAUSE_MS)
  }

  await writeJournal(journal)

  stat.tookMs = Date.now() - startedAll
  console.log(`Имена: добавлено ${stat.added} за ${round1(stat.tookMs / 60000)} мин`)

  return { names, stat }
}
