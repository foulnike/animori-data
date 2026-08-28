// Волна карты: соответствия MAL — AniList дополняются с самого AniList.
//
// ЗАЧЕМ. У манами пара есть не у каждой записи: в первом выпуске 18 858 пар
// на 30 215 имён. Без пары клиент не свяжет свой номер AniList с именем,
// которое лежит в файле по номеру MAL, и уйдёт в сеть за тем, что уже есть
// на диске. Больнее всего это в поиске и на полках главной: там тайтлы чужие,
// своей записи списка с номером MAL у них нет, и карта — единственный мост.
//
// Спрашиваем только недостающие: пары манами перепроверять незачем, они
// собраны из тех же ссылок, что и номера.
//
// ВОЛНА НЕОБЯЗАТЕЛЬНАЯ. AniList уже уходил в отказ на трое суток, и недельная
// сборка не вправе от него зависеть. Что нашлось — доедет в выпуск, что нет —
// останется на парах манами, и выпуск выйдет как прежде.

import { bump, pct, round1, sleep, why } from './common.mjs'

const ENDPOINT = 'https://graphql.anilist.co'
/** Потолок страницы у AniList — пятьдесят записей за запрос. */
const BATCH = 50
/** Потолок одного запроса: виснувшее соединение не должно съесть прогон. */
const TIMEOUT_MS = 15000
/** 1200 мс — это полсотни запросов в минуту при разрешённых девяноста. */
const PAUSE_MS = Number(process.env.ANILIST_PAUSE || 1200)
/** После скольких отказов подряд волна сдаётся и отдаёт найденное. */
const GIVE_UP_AFTER = 5
/** Через сколько запросов печатается строка о ходе дела. */
const REPORT_EVERY = 25
const UA = 'AniMori/3.0 (+https://github.com/foulnike/animori-data)'

/**
 * Вид вписан словом, а не переменной: отбор по номерам чужой вид не исключает,
 * и ошибка в переменной тихо притащила бы в карту мангу.
 */
const QUERY = `query ($ids: [Int], $perPage: Int!) {
  Page(page: 1, perPage: $perPage) {
    media(idMal_in: $ids, type: ANIME) {
      id
      idMal
    }
  }
}`

/** Один запрос. Отказ не бросается: волна разбирает его сама. */
async function ask(ids) {
  const started = Date.now()

  try {
    const answer = await fetch(ENDPOINT, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        accept: 'application/json',
        'user-agent': UA,
      },
      body: JSON.stringify({ query: QUERY, variables: { ids, perPage: BATCH } }),
      signal: AbortSignal.timeout(TIMEOUT_MS),
    })
    const took = Date.now() - started

    if (answer.status !== 200) {
      const retryAfter = Number(answer.headers.get('retry-after') || 0)
      return { status: answer.status, took, retryAfter }
    }

    const body = await answer.json()
    const media = body && body.data && body.data.Page ? body.data.Page.media : null
    if (!Array.isArray(media)) return { status: 'ответ без списка', took }
    return { status: 200, took, media }
  } catch (e) {
    return { status: `сеть: ${why(e)}`, took: Date.now() - started }
  }
}

/**
 * Дополняет пары манами. Возвращает новый список пар и статистику волны.
 *
 * @param malIds все номера MAL, известные из манами
 * @param pairs пары манами вида [номер MAL, номер AniList]
 */
export async function enrichMap(malIds, pairs) {
  const stat = { skipped: false, asked: 0, requests: 0, added: 0, codes: {}, tookMs: 0, gaveUp: false }
  const startedAll = Date.now()

  if (process.env.BUILD_ANILIST === 'off') {
    console.log('Карта: волна AniList выключена настройкой')
    stat.skipped = true
    return { pairs, stat }
  }

  const paired = new Set(pairs.map(([mal]) => mal))
  const missing = malIds.filter((mal) => !paired.has(mal))

  console.log(`Карта: пар от манами ${pairs.length}, спросим AniList про ${missing.length}`)
  if (missing.length === 0) return { pairs, stat }

  const found = []
  let at = 0
  let misses = 0

  while (at < missing.length) {
    const batch = missing.slice(at, at + BATCH)
    let answer = await ask(batch)

    // Окно лимита у AniList минутное: меньше минуты ждать бессмысленно.
    if (answer.status === 429) {
      const wait = Math.max(answer.retryAfter * 1000, 60000)
      console.log(`AniList: 429, ждём ${wait} мс и повторяем ту же пачку`)
      stat.requests++
      bump(stat.codes, 429)
      await sleep(wait)
      answer = await ask(batch)
    }

    stat.requests++
    bump(stat.codes, answer.status)

    if (answer.status !== 200) {
      misses++
      console.log(`AniList: пачка ${1 + at / BATCH} — ${answer.status}`)

      if (misses >= GIVE_UP_AFTER) {
        stat.gaveUp = true
        console.log(`AniList: ${GIVE_UP_AFTER} отказов подряд, волна свёрнута`)
        break
      }

      await sleep(PAUSE_MS)
      continue
    }

    misses = 0
    stat.asked += batch.length

    for (const item of answer.media) {
      if (!item) continue
      const id = item.id
      const idMal = item.idMal
      if (typeof id !== 'number' || typeof idMal !== 'number') continue
      if (id <= 0 || idMal <= 0) continue
      // Повтор возможен: два тайтла AniList изредка ссылаются на один номер MAL.
      if (paired.has(idMal)) continue
      paired.add(idMal)
      found.push([idMal, id])
    }

    at += BATCH

    if (stat.requests % REPORT_EVERY === 0) {
      const done = Math.min(at, missing.length)
      console.log(`Карта: ${done} из ${missing.length} (${pct(done / missing.length)}), нашли ${found.length}`)
    }

    await sleep(PAUSE_MS)
  }

  stat.added = found.length
  stat.tookMs = Date.now() - startedAll
  console.log(`Карта: добавлено ${found.length} пар за ${round1(stat.tookMs / 60000)} мин`)

  return { pairs: pairs.concat(found), stat }
}
