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
// ПЕРЕСБОРКА ОПИРАЕТСЯ НА СЕМЯ, А НЕ ОТМЕНЯЕТ ЕГО. Пересборка нужна затем,
// что пары не только появляются: запись AniList может переехать на другой
// номер или исчезнуть вовсе. Раньше в такой прогон семя не шло вообще,
// и результатом была ровно та карта, которую волна успела собрать за раз.
// Значит исходов было два: полная удача или полная потеря — недособранная
// пересборка роняла прогон на пороге приёмки, и час работы уходил в ничто.
// Хуже того, пара исчезала по умолчанию молчания: «AniList сказал, что такого
// нет» и «AniList не успели спросить» давали один и тот же итог, хотя это
// разные факты.
//
// Теперь семя в пересборку идёт всегда и служит опорой: волна спрашивает
// про все номера, но пару выбрасывает только тогда, когда пачка ответила
// успешно и пары в ответе не оказалось. Номер, до которого волна не дошла,
// остаётся как в семени. Отсюда главное свойство: пересборка не бывает хуже
// семени ни при каком сроке, отказе или лимите. Оборванная пересборка — это
// просто пересборка, сделанная наполовину, и её можно продолжить на следующей
// неделе.
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
// СРОК, А НЕ УДАЧА. Волна принимает срок — время, к которому она обязана
// вернуть управление, чего бы ни успела. Раньше срока не было вовсе, и
// остановить волну могли только пять отказов подряд; при перемешанных 429
// и удачах счётчик обнулялся на каждом успехе и не срабатывал никогда.
// Кончалось это отменой всего задания по потолку прогона: терялся и обход,
// и волна имён, и файлы, которых никто так и не записал. Теперь предел
// времени — обычный исход волны, а не авария: она сворачивается сама,
// говорит об этом словами и отдаёт собранное.
//
// ЛИМИТ AniList РАЗБИРАЕТСЯ ПО ОТВЕТУ, А НЕ ПО ЧАСАМ. Прежняя мера была одна:
// на любой 429 спать минуту и повторять ту же пачку. Минута бралась из того,
// что окно лимита минутное, но платилась она и там, где хватило бы пяти
// секунд, а пачка после сна повторялась в том же темпе, который её и уронил.
// Живой прогон на этом провёл в ожидании около девяноста минут из ста.
//
// Мер теперь три, и все дешёвые. Первая: Retry-After, если AniList его дал,
// а иначе лестница пять — десять — двадцать — сорок секунд, и только потом
// минута. Вторая: пауза между запросами растёт после каждого отказа и
// отпускается назад после череды чистых пачек — темп находится сам, вместо
// того чтобы биться о потолок ровными двумя секундами. Третья: остаток окна
// читается из X-RateLimit-Remaining, и на исходе волна ждёт смену окна
// заранее — предупредить отказ дешевле, чем его переждать.
//
// ОТКАЗ СТОРОННЕМУ ДОСТУПУ (сентябрь 2026). AniList отвечает 403 с телом
// «The AniList API has been temporarily disabled due to severe stability
// issues» на запрос без Origin и Referer своей страницы. Тот же запрос
// с браузерными заголовками отвечает 200 — дело не в адресе и не в частоте.
// Обходить фильтр отсюда нельзя: прогон идёт на общих адресах раннеров
// GitHub, документированная мера AniList — ручная блокировка адреса,
// и поймать её значит подставить чужих, даже не узнав об этом. Волна такой
// отказ узнаёт по телу и сворачивается сразу, не тратя попытки и паузы
// между ними.

import { bump, pct, round1, sleep, why } from './common.mjs'

const ENDPOINT = 'https://graphql.anilist.co'
/** Потолок страницы у AniList — пятьдесят записей за запрос. */
const BATCH = 50
/** Потолок одного запроса: виснувшее соединение не должно съесть прогон. */
const TIMEOUT_MS = 15000
/**
 * Начальная пауза между запросами. 2500 мс — это двадцать четыре запроса
 * в минуту при разрешённых тридцати.
 *
 * Здесь было 1200 мс, посчитанные на девяносто в минуту из документации.
 * Замер живого ответа дал X-RateLimit-Limit=30 и Remaining=29: лимит урезан
 * втрое как временная мера ещё в 2022 году и так и не вернулся. Потом стояло
 * 2100 мс — двадцать восемь в минуту, то есть жизнь ровно на кромке потолка:
 * первый же всплеск давал 429 за 429, и живой прогон 13 сентября 2026 собрал
 * их восемьдесят восемь, проведя в ожидании полтора часа.
 *
 * Четыре запроса в минуту запаса стоят полутора минут на полной пересборке
 * и снимают почти все отказы. Обычная неделя стоит десятки запросов, и
 * разницы в ней не видно вовсе.
 *
 * Число начальное: дальше волна ведёт паузу сама, см. PAUSE_MAX_MS ниже.
 * Приложение считает свой темп само и с другого конца: см. anilistLimiter
 * и ANILIST_START_PER_WINDOW в src/shared/api/rate-limit.ts. Общего
 * ограничителя у сборки и клиента нет и быть не может: это разные процессы
 * на разных машинах, и при правке одного места второе надо править руками.
 */
const PAUSE_MS = Number(process.env.ANILIST_PAUSE || 2500)
/**
 * Потолок паузы: шесть запросов в минуту. Ниже этого темпа падать незачем —
 * если AniList отказывает и здесь, дело не в частоте, и лечится оно сроком
 * волны, а не новым замедлением.
 */
const PAUSE_MAX_MS = 10000
/** Во сколько раз пауза растёт после отказа по лимиту. */
const PAUSE_UP = 1.4
/** И во сколько отпускается назад после череды чистых пачек. */
const PAUSE_DOWN = 0.9
/** Сколько пачек подряд должны пройти чисто, чтобы ослабить паузу. */
const CALM_BATCHES = 10
/** Остаток окна, при котором волна ждёт его смену, не дожидаясь 429. */
const GUARD_REMAINING = 3
/** Окно лимита у AniList минутное. */
const WINDOW_MS = 60000
/** Первая ступень ожидания после 429, если Retry-After не пришёл. */
const WAIT_START_MS = 5000
/** Потолок одного ожидания: дольше минуты ждать минутное окно бессмысленно. */
const WAIT_CAP_MS = 65000
/** Сколько раз повторяется одна пачка, прежде чем волна пойдёт дальше. */
const TRIES_PER_BATCH = 4
/** После скольких не ответивших пачек подряд волна сдаётся. */
const GIVE_UP_AFTER = 8
/** И сколько их всего терпится за прогон: битое окно лучше пережить сроком. */
const GIVE_UP_TOTAL = 40
/** Через сколько пачек печатается строка о ходе дела. */
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

    // Остаток окна лимита. Заголовка может не быть вовсе — тогда undefined,
    // и предупредительная пауза просто не работает. Отличать ноль от
    // отсутствия обязательно: Number(null) даёт ноль, и волна спала бы
    // по минуте после каждой пачки.
    const rawLeft = answer.headers.get('x-ratelimit-remaining')
    const remaining = rawLeft === null || rawLeft === '' ? undefined : Number(rawLeft)

    if (answer.status !== 200) {
      const retryAfter = Number(answer.headers.get('retry-after') || 0)
      // Тело отказа читается намеренно. У 403 в нём лежит причина, и без неё
      // волна не отличит «доступ закрыт всем» от случайной помехи шлюза:
      // первое не лечится ни повтором, ни паузой, второе лечится обоими.
      const text = await answer.text().catch(() => '')
      return { status: answer.status, took, retryAfter, text, remaining }
    }

    const body = await answer.json()
    const media = body && body.data && body.data.Page ? body.data.Page.media : null
    if (!Array.isArray(media)) return { status: 'ответ без списка', took, remaining }
    return { status: 200, took, media, remaining }
  } catch (e) {
    return { status: `сеть: ${why(e)}`, took: Date.now() - started }
  }
}

/**
 * Спрашивает пары у AniList. Возвращает новый список пар и статистику волны.
 *
 * @param malIds все номера MAL из перечисления каталога
 * @param pairs семя — пары вида [номер MAL, номер AniList] из карты прошлого
 *   выпуска. Пустое семя законно и означает либо первый прогон, либо неудачу
 *   скачивания: тогда спрашивать придётся про всё.
 * @param options.deadline время в мс, к которому волна обязана вернуть
 *   управление. Без него волна идёт до конца очереди.
 * @param options.rebuild спрашивать про все номера, а не только про те,
 *   которых в семени нет. Семя при этом остаётся опорой: пара уходит только
 *   по успешному ответу без неё.
 */
export async function enrichMap(malIds, pairs, options = {}) {
  const deadline = Number(options.deadline) || Infinity
  const rebuild = options.rebuild === true

  const stat = {
    skipped: false,
    seeded: pairs.length,
    asked: 0,
    requests: 0,
    added: 0,
    changed: 0,
    dropped: 0,
    kept: 0,
    throttled: 0,
    waitedMs: 0,
    failed: 0,
    codes: {},
    tookMs: 0,
    gaveUp: false,
    disabled: false,
    reason: '',
    pause: PAUSE_MS,
  }
  const startedAll = Date.now()

  if (process.env.BUILD_ANILIST === 'off') {
    console.log('Карта: волна AniList выключена настройкой')
    stat.skipped = true
    return { pairs, stat }
  }

  // Пары держатся в Map, а не в множестве и массиве: в пересборке волна
  // не только добавляет пары, но и правит, и убирает.
  const known = new Map()
  for (const [mal, anilist] of pairs) known.set(mal, anilist)

  const queue = rebuild ? malIds.slice() : malIds.filter((mal) => !known.has(mal))

  const minutes = round1((queue.length / BATCH) * (PAUSE_MS / 60000))
  const left = deadline === Infinity ? null : round1((deadline - Date.now()) / 60000)
  console.log(
    `Карта: готовых пар ${known.size}, ` +
      (rebuild
        ? `переспросим AniList про все ${queue.length}`
        : `спросим AniList про ${queue.length}`) +
      ` (около ${Math.ceil(queue.length / BATCH)} запросов, ~${minutes} мин` +
      (left === null ? '' : `, срок ${left} мин`) +
      ')',
  )

  if (queue.length === 0) {
    console.log('Карта: новых номеров нет, спрашивать нечего')
    stat.tookMs = Date.now() - startedAll
    return { pairs, stat }
  }

  // Номера, про которые AniList успел ответить. Нужны затем, чтобы отличить
  // «ответа не было» от «ответ был, пары в нём нет».
  const touched = new Set()

  let at = 0
  let misses = 0
  let calm = 0
  let batches = 0
  let pause = PAUSE_MS
  let wait = WAIT_START_MS

  while (at < queue.length) {
    if (Date.now() >= deadline) {
      stat.gaveUp = true
      stat.reason = 'срок волны вышел'
      console.log(`Карта: срок волны вышел, спрошено ${at} из ${queue.length}`)
      break
    }

    const batch = queue.slice(at, at + BATCH)
    const number = 1 + at / BATCH
    batches++

    let answer = null
    let ok = false

    for (let tries = 1; tries <= TRIES_PER_BATCH; tries++) {
      answer = await ask(batch)
      stat.requests++
      bump(stat.codes, answer.status)

      if (answer.status === 200) {
        ok = true
        break
      }

      // Закрытый доступ всем сторонним — состояние сервиса, а не помеха
      // на пачке: ответ будет тот же до конца прогона, повторять нечего.
      if (answer.status === 403 && String(answer.text || '').includes(DISABLED_MARK)) break

      if (answer.status === 429) {
        stat.throttled++
        calm = 0
        pause = Math.min(Math.round(pause * PAUSE_UP), PAUSE_MAX_MS)

        const asked = Number(answer.retryAfter) > 0 ? answer.retryAfter * 1000 : wait
        const nap = Math.min(asked, WAIT_CAP_MS)
        wait = Math.min(wait * 2, WAIT_CAP_MS)

        console.log(
          `AniList: 429 на пачке ${number}, ждём ${round1(nap / 1000)} с, ` +
            `пауза теперь ${pause} мс (попытка ${tries} из ${TRIES_PER_BATCH})`,
        )
        stat.waitedMs += nap
        await sleep(nap)
        if (Date.now() >= deadline) break
        continue
      }

      console.log(
        `AniList: пачка ${number} — ${answer.status} (попытка ${tries} из ${TRIES_PER_BATCH})`,
      )
      await sleep(pause)
    }

    if (!ok) {
      if (answer && answer.status === 403 && String(answer.text || '').includes(DISABLED_MARK)) {
        stat.disabled = true
        stat.gaveUp = true
        stat.reason = 'сторонний доступ к AniList закрыт'
        console.log('AniList: сторонний доступ к API закрыт, волна свёрнута сразу')
        break
      }

      if (Date.now() >= deadline) {
        stat.gaveUp = true
        stat.reason = 'срок волны вышел'
        console.log(`Карта: срок волны вышел на пачке ${number}, спрошено ${at} из ${queue.length}`)
        break
      }

      // Пачка не ответила — идём дальше. Её номера остаются неспрошенными,
      // и в пересборке это значит «оставить как в семени»: молчание AniList
      // отсутствия пары не доказывает. Раньше волна повторяла ту же пачку
      // без счёта и без сдвига, и на этом стояла до конца прогона.
      stat.failed++
      misses++
      at += BATCH

      if (misses >= GIVE_UP_AFTER) {
        stat.gaveUp = true
        stat.reason = `${GIVE_UP_AFTER} пачек подряд без ответа`
        console.log(`AniList: ${GIVE_UP_AFTER} пачек подряд без ответа, волна свёрнута`)
        break
      }

      if (stat.failed >= GIVE_UP_TOTAL) {
        stat.gaveUp = true
        stat.reason = `${GIVE_UP_TOTAL} пачек без ответа за прогон`
        console.log(`AniList: ${GIVE_UP_TOTAL} пачек без ответа за прогон, волна свёрнута`)
        break
      }

      await sleep(pause)
      continue
    }

    misses = 0
    wait = WAIT_START_MS
    stat.asked += batch.length

    // Ответ разбирается в карту: повтор возможен, два тайтла AniList изредка
    // ссылаются на один номер MAL. Берётся первый — он и раньше брался первым.
    const answered = new Map()
    for (const item of answer.media) {
      if (!item) continue
      const id = item.id
      const idMal = item.idMal
      if (typeof id !== 'number' || typeof idMal !== 'number') continue
      if (id <= 0 || idMal <= 0) continue
      if (answered.has(idMal)) continue
      answered.set(idMal, id)
    }

    for (const mal of batch) {
      touched.add(mal)
      const found = answered.get(mal)
      const had = known.get(mal)

      if (found === undefined) {
        // Успешный ответ без пары — доказанное отсутствие, а не молчание.
        // Только здесь пару и можно убрать.
        if (had !== undefined) {
          known.delete(mal)
          stat.dropped++
        }
        continue
      }

      if (had === undefined) {
        known.set(mal, found)
        stat.added++
        continue
      }

      // Запись AniList переехала на другой номер. Редко, но именно за этим
      // пересборку и просят.
      if (had !== found) {
        known.set(mal, found)
        stat.changed++
      }
    }

    at += BATCH

    // Пауза отпускается назад после череды чистых пачек: иначе один всплеск
    // в начале держал бы волну медленной до самого конца.
    calm++
    if (calm >= CALM_BATCHES && pause > PAUSE_MS) {
      pause = Math.max(PAUSE_MS, Math.round(pause * PAUSE_DOWN))
      calm = 0
    }

    if (batches % REPORT_EVERY === 0) {
      const done = Math.min(at, queue.length)
      console.log(
        `Карта: ${done} из ${queue.length} (${pct(done / queue.length)}), пар ${known.size}, ` +
          `добрано ${stat.added}` +
          (rebuild ? `, убрано ${stat.dropped}` : ''),
      )
    }

    // Остаток окна на исходе — ждём его смену заранее. Предупредить отказ
    // дешевле, чем его переждать: 429 стоит и ожидания, и повтора пачки.
    if (Number.isFinite(answer.remaining) && answer.remaining <= GUARD_REMAINING) {
      console.log(`AniList: в окне осталось ${answer.remaining} запросов, ждём смену окна`)
      stat.waitedMs += WINDOW_MS
      await sleep(WINDOW_MS)
      continue
    }

    await sleep(pause)
  }

  // Сколько номеров семени волна так и не переспросила. Их пары остались
  // как были — это и есть опора, из-за которой пересборка не бывает хуже
  // семени.
  if (rebuild) {
    for (const [mal] of pairs) if (!touched.has(mal)) stat.kept++
  }

  stat.pause = pause
  stat.tookMs = Date.now() - startedAll

  const result = []
  for (const [mal, anilist] of known) result.push([mal, anilist])

  console.log(
    `Карта: пар ${result.length} за ${round1(stat.tookMs / 60000)} мин ` +
      `(добрано ${stat.added}, переехало ${stat.changed}, убрано ${stat.dropped}` +
      (rebuild ? `, оставлено от семени ${stat.kept}` : '') +
      ')',
  )

  if (stat.throttled > 0) {
    console.log(
      `Карта: отказов по лимиту ${stat.throttled}, в ожидании ` +
        `${round1(stat.waitedMs / 60000)} мин, пауза к концу ${pause} мс`,
    )
  }

  return { pairs: result, stat }
}
