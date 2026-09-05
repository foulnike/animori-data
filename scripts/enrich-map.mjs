// Волна карты: соответствия MAL — AniList спрашиваются у самого AniList.
//
// ЗАЧЕМ. Файл имён лежит по номерам MAL, а клиент живёт на номерах AniList.
// Без пары он не свяжет одно с другим и уйдёт в сеть за тем, что уже есть
// на диске. Больнее всего это в поиске и на полках главной: там тайтлы чужие,
// своей записи списка с номером MAL у них нет, и карта — единственный мост.
//
// ПОЧЕМУ СПРАШИВАЕМ ТОЛЬКО ПРО ОСТАТОК. Готовые пары приходят семенем — это
// карта прошлого выпуска, сборщик качает её сам. Спрашивать приходится лишь
// про номера, которых в семени нет: сотни вместо тридцати тысяч, десятки
// запросов вместо шестисот десяти, минута вместо двадцати одной. Так было
// и раньше, пока пары даром отдавала манами; разница в том, что семя теперь
// своё — оно родится из прошлого прогона, а не из чужого архива.
//
// ОКНО ВЫДАЧИ НАС НЕ ЗАДЕВАЕТ. У AniList выборка постранично ограничена
// пятью тысячами записей: страница 101 отвечает четырестами, а total и lastPage
// в PageInfo объявлены неточными самой документацией. Поэтому перечислить
// каталог AniList невозможно вовсе, и шаг четырёх тысяч номеров — вымысел.
// Здешний запрос всегда берёт page: 1 и отбирает по idMal_in, то есть в окно
// не упирается ни при каком размере каталога. Менять page здесь нельзя.
//
// ВОЛНА НЕОБЯЗАТЕЛЬНАЯ. AniList уже уходил в отказ на трое суток, и недельная
// сборка не вправе от него зависеть. Что нашлось — доедет в выпуск, что нет —
// останется на следующую неделю, и выпуск выйдет как прежде. Пустой карта при
// этом больше не остаётся: в выпуск уходит семя, то есть карта прошлого
// выпуска как есть. Плата за это — карта умеет застывать незаметно, ровно как
// застыл бы вход на манами. Поэтому её возраст сборщик называет в описи и
// в описании выпуска, а не прячет за неизменившимся числом пар.
//
// ОТКАЗ СТОРОННЕМУ ДОСТУПУ (сентябрь 2026). AniList отвечает 403 с телом
// «The AniList API has been temporarily disabled due to severe stability
// issues» на запрос без Origin и Referer своей страницы. Тот же запрос
// с браузерными заголовками отвечает 200 — дело не в адресе и не в частоте.
// Обходить фильтр отсюда нельзя: прогон идёт на общих адресах раннеров
// GitHub, документированная мера AniList — ручная блокировка адреса,
// и поймать её значит подставить чужих, даже не узнав об этом. Волна такой
// отказ узнаёт по телу и сворачивается сразу, не тратя пять пачек и паузы
// между ними.

import { bump, pct, round1, sleep, why } from './common.mjs'

const ENDPOINT = 'https://graphql.anilist.co'
/** Потолок страницы у AniList — пятьдесят записей за запрос. */
const BATCH = 50
/** Потолок одного запроса: виснувшее соединение не должно съесть прогон. */
const TIMEOUT_MS = 15000
/**
 * 2100 мс — это двадцать восемь запросов в минуту при разрешённых тридцати.
 *
 * Здесь было 1200 мс, посчитанные на девяносто в минуту из документации.
 * Замер живого ответа дал X-RateLimit-Limit=30 и Remaining=29: лимит урезан
 * втрое как временная мера ещё в 2022 году и так и не вернулся. При 1200 мс
 * волна шла вдвое быстрее потолка, то есть жила на 429 и рисковала
 * свернуться по GIVE_UP_AFTER, потеряв всю недособранную часть карты.
 *
 * Поверх минутного окна у AniList есть отдельный ограничитель всплесков,
 * поэтому ровные две секунды лучше пачек без пауз.
 *
 * Приложение считает то же число само и с другого конца: см. anilistLimiter
 * и ANILIST_START_PER_WINDOW в src/shared/api/rate-limit.ts. Общего ограничителя
 * у сборки и клиента нет и быть не может: это разные процессы на разных
 * машинах, и при правке одного места второе надо править руками.
 */
const PAUSE_MS = Number(process.env.ANILIST_PAUSE || 2100)
/** После скольких отказов подряд волна сдаётся и отдаёт найденное. */
const GIVE_UP_AFTER = 5
/** Через сколько запросов печатается строка о ходе дела. */
const REPORT_EVERY = 25
const UA = 'AniMori/3.0 (+https://github.com/foulnike/animori-data)'
/**
 * Примета отказа всему стороннему доступу. Ищется подстрокой намеренно:
 * вокруг неё AniList формулировку уже менял, а «temporarily disabled»
 * держится с первого дня отказа.
 */
const DISABLED_MARK = 'temporarily disabled'

/**
 * Вид вписан словом, а не переменной: отбор по номерам чужой вид не исключает,
 * и ошибка в переменной тихо притащила бы в карту мангу.
 *
 * page: 1 тоже вписана числом намеренно: отбор идёт по списку номеров,
 * пачка сама не больше пятидесяти, и вторая страница ответа не бывает
 * нужна никогда. Заодно это уводит волну от окна выдачи в пять тысяч записей.
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
      // Тело отказа читается намеренно. У 403 в нём лежит причина, и без неё
      // волна не отличит «доступ закрыт всем» от случайной помехи шлюза:
      // первое не лечится ни повтором, ни паузой, второе лечится обоими.
      const text = await answer.text().catch(() => '')
      return { status: answer.status, took, retryAfter, text }
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
 * Спрашивает пары у AniList. Возвращает новый список пар и статистику волны.
 *
 * @param malIds все номера MAL из перечисления каталога
 * @param pairs семя — пары вида [номер MAL, номер AniList] из карты прошлого
 *   выпуска. Волна спрашивает только про номера, которых в семени нет, и
 *   возвращает семя вместе с добранным. Пустое семя законно и означает либо
 *   первый прогон, либо неудачу скачивания: тогда спрашивать придётся про всё.
 */
export async function enrichMap(malIds, pairs) {
  const stat = {
    skipped: false,
    seeded: pairs.length,
    asked: 0,
    requests: 0,
    added: 0,
    codes: {},
    tookMs: 0,
    gaveUp: false,
    disabled: false,
  }
  const startedAll = Date.now()

  if (process.env.BUILD_ANILIST === 'off') {
    console.log('Карта: волна AniList выключена настройкой')
    stat.skipped = true
    return { pairs, stat }
  }

  const paired = new Set(pairs.map(([mal]) => mal))
  const missing = malIds.filter((mal) => !paired.has(mal))

  const minutes = round1((missing.length / BATCH) * (PAUSE_MS / 60000))
  console.log(
    `Карта: готовых пар ${pairs.length}, спросим AniList про ${missing.length} ` +
      `(около ${Math.ceil(missing.length / BATCH)} запросов, ~${minutes} мин)`,
  )
  if (missing.length === 0) {
    console.log('Карта: новых номеров нет, спрашивать нечего')
    stat.tookMs = Date.now() - startedAll
    return { pairs, stat }
  }

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

      // Закрытый сторонний доступ — состояние сервиса, а не помеха на пачке.
      // Ответ будет тот же до конца прогона, поэтому волна уходит сразу:
      // остальные четыре пачки и паузы между ними ничего бы не изменили.
      if (answer.status === 403 && String(answer.text || '').includes(DISABLED_MARK)) {
        stat.disabled = true
        stat.gaveUp = true
        console.log('AniList: сторонний доступ к API закрыт, волна свёрнута сразу')
        break
      }

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
