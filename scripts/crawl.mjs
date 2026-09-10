// Обход каталога Шикимори для сборщика датасета.
//
// Здесь живёт всё, что касается сети и страниц: выбор зеркала, перечисление
// до пустой страницы и два вида обхода. Сборщику остаются пороги приёмки,
// волны обогащения и запись файлов.
//
// ДВА ОБХОДА. Полный перечисляет каталог целиком — 610 страниц по замеру
// сентября 2026. Раньше так шла каждая неделя, и почти весь этот труд был
// напрасен: за неделю каталог прирастает десятками записей, не тысячами.
// Частичный берёт семенем файл имён прошлого выпуска и спрашивает только
// то, что впрямь могло измениться.
//
// ЧТО СПРАШИВАЕТ ЧАСТИЧНЫЙ. Во-первых, хвост каталога: порядок order=id
// восходящий, поэтому новое лежит в конце, и обход начинается за TAIL_SLACK_PAGES
// страниц до головы семени — с запасом на записи, пропавшие из каталога:
// каталог не только растёт, и тогда хвост уезжает назад. Во-вторых, весь
// список идущего и анонсов (status=ongoing,anons): у этих записей меняется всё —
// и название, и дата выхода, и само состояние. Всё прочее в каталоге по
// определению вышло и больше не меняется; редкие правки названий у вышедшего
// подбирает очередной полный обход.
//
// ПОЧЕМУ СЧЁТЧИКИ СОСТОЯНИЙ ОСТАЮТСЯ ТОЧНЫМИ. Состояний ровно три. Идущее
// и анонсы частичный обход перечисляет целиком, значит вышедшее — это весь
// остаток каталога. Наследовать счётчики из прошлой описи не приходится ни
// разу — а это важно: унаследованное число молча застыло бы, а сторож смотрит
// именно на них.
//
// Зависимостей нет намеренно: всё нужное есть в Node из коробки.

import { bump, sleep, why } from './common.mjs'

/**
 * Потолок Шикимори на одну страницу. limit=100 не ошибка, но и не работает:
 * замер показал, что он молча приводится к пятидесяти.
 */
export const BATCH = 50
/**
 * Адреса по порядку. Замер сентября 2026: rip не отказал ни разу из четырёх
 * проб, io отказал один раз из шести, one — дважды из четырёх.
 */
export const MIRRORS = ['shikimori.rip', 'shikimori.io', 'shikimori.one']
/** После скольких отказов подряд обход уходит на следующий адрес. */
const SWITCH_AFTER = 3
/** Осмысленный User-Agent обязателен: без него Шикимори отвечает отказом. */
export const UA = 'AniMori/3.0 (+https://github.com/foulnike/animori-data)'
const SHIKI_PATH = '/api/animes'
/** Потолок одного запроса: виснувшее соединение не должно съесть весь прогон. */
export const TIMEOUT_MS = 15000
/**
 * Предохранитель от бесконечного перечисления. Отсечки по смещению у Шикимори
 * нет вовсе: за концом каталога приходит пустой массив, а не ошибка. Упёрлись
 * в две тысячи страниц — сломалось условие остановки, и сборка обязана упасть.
 */
const MAX_PAGES = 2000
/**
 * Запас страниц перед головой каталога в частичном обходе: сто записей
 * покрывают обычную уборку каталога с избытком.
 */
const TAIL_SLACK_PAGES = 2
/** Через сколько страниц печатается строка о ходе дела. */
const REPORT_EVERY = 50

const PAUSE_MS = Number(process.env.BUILD_PAUSE || 700)

/**
 * Одна страница каталога. Куки не шлём — клиент тоже не шлёт: с ними бывает 400.
 * order=id даёт устойчивый порядок, censored=false — полный каталог.
 *
 * ЛОВУШКА ЦЕНЗУРЫ. Без censored=false каталог молча теряет около шести тысяч
 * записей: цензурированный кончается между смещениями 23 000 и 24 990, а полный
 * идёт до 30 471. Убирать нельзя.
 *
 * Отбор по состоянию приходит доводом: частичный обход спрашивает им идущее.
 */
async function ask(domain, page, filter) {
  const url =
    'https://' +
    domain +
    SHIKI_PATH +
    `?page=${page}&limit=${BATCH}&order=id&censored=false` +
    (filter === '' ? '' : `&${filter}`)
  const started = Date.now()

  try {
    const answer = await fetch(url, {
      headers: { 'user-agent': UA, accept: 'application/json' },
      signal: AbortSignal.timeout(TIMEOUT_MS),
    })
    const took = Date.now() - started

    if (answer.status !== 200) {
      const retryAfter = Number(answer.headers.get('retry-after') || 0)
      return { status: answer.status, took, retryAfter }
    }

    const items = await answer.json()
    if (!Array.isArray(items)) return { status: 'ответ не массив', took }
    return { status: 200, took, items }
  } catch (e) {
    return { status: `сеть: ${why(e)}`, took: Date.now() - started }
  }
}

/** Одна запись в том виде, в каком она ложится в файл: ровно шесть полей. */
export function rowOf(item) {
  return {
    id: item.id,
    name: item.name,
    russian: (item.russian || '').trim(),
    kind: item.kind,
    aired_on: item.aired_on,
    score: item.score,
  }
}

/**
 * Пустое состояние обхода. Одно на весь прогон, включая оба перечисления
 * частичного обхода: уход на запасное зеркало в первом обязан помниться
 * во втором, иначе сборщик снова пойдёт в ту же закрытую дверь.
 */
export function newStat() {
  return {
    mirror: MIRRORS[0],
    mirrorAt: 0,
    requests: 0,
    pages: 0,
    codes: {},
    tookMs: 0,
  }
}

/**
 * Перечисление страниц подряд с указанной и до пустой. Страница не бросается
 * при отказе: она повторяется, а после трёх отказов подряд обход уходит на
 * следующий адрес. Прогон, где первое зеркало молчало на каждой пачке, уже
 * был на пробе — сборка обязана это пережить.
 *
 * При полном отказе всех зеркал зовётся onFail: решать судьбу прогона — дело
 * сборщика, а не обхода; здесь нет ни порогов приёмки, ни итога прогона.
 */
export async function sweep(stat, filter, fromPage, onItem, onFail) {
  let page = fromPage
  let misses = 0

  for (;;) {
    if (page > MAX_PAGES) {
      onFail(`перечисление не кончилось за ${MAX_PAGES} страниц: условие остановки сломано`)
      return
    }

    const domain = MIRRORS[stat.mirrorAt]
    let answer = await ask(domain, page, filter)

    // Один повтор на 429: лимит говорит «подожди», а не «уходи».
    if (answer.status === 429) {
      const wait = Math.max(answer.retryAfter * 1000, 5000)
      console.log(`${domain}: 429, ждём ${wait} мс и повторяем ту же страницу`)
      stat.requests++
      bump(stat.codes, 429)
      await sleep(wait)
      answer = await ask(domain, page, filter)
    }

    stat.requests++
    bump(stat.codes, answer.status)

    if (answer.status !== 200) {
      misses++
      console.log(`${domain}: страница ${page} — ${answer.status}`)

      if (misses >= SWITCH_AFTER) {
        stat.mirrorAt++
        misses = 0
        if (stat.mirrorAt >= MIRRORS.length) {
          onFail(`все адреса Шикимори перестали отвечать на странице ${page}`)
          return
        }
        stat.mirror = MIRRORS[stat.mirrorAt]
        console.log(`Уходим на ${stat.mirror}: ${domain} не отвечает`)
      }

      await sleep(PAUSE_MS)
      continue
    }

    misses = 0

    // Пустая страница — законный конец перечисления, а не отказ.
    if (answer.items.length === 0) {
      console.log(`Перечисление кончилось: страница ${page} пуста`)
      break
    }

    stat.pages++

    for (const item of answer.items) {
      if (!Number.isFinite(item.id) || item.id <= 0) continue
      onItem(item)
    }

    if (stat.pages % REPORT_EVERY === 0) {
      console.log(`Обход: страница ${page}, запросов ${stat.requests}`)
    }

    page++
    await sleep(PAUSE_MS)
  }
}

/**
 * Полный обход: весь каталог подряд. Идёт в первый прогон, при потере семени
 * и раз в месяц: правки названий у вышедшего частичный обход не видит.
 */
export async function crawlFull(onFail) {
  const stat = newStat()
  const rows = []
  const seen = new Set()
  const ongoing = new Set()
  const anons = new Set()
  const started = Date.now()

  await sweep(
    stat,
    '',
    1,
    (item) => {
      // Страницы могут перекрыться, если каталог пополнился прямо во время
      // обхода: order=id держит порядок, но новая запись сдвигает хвост.
      if (seen.has(item.id)) return
      seen.add(item.id)

      if (item.status === 'ongoing') ongoing.add(item.id)
      else if (item.status === 'anons') anons.add(item.id)

      rows.push(rowOf(item))
    },
    onFail,
  )

  stat.tookMs = Date.now() - started
  return { stat, rows, ongoing, anons, mode: 'full', added: rows.length, changed: 0 }
}

/**
 * Частичный обход: семя прошлого выпуска плюс два коротких перечисления.
 *
 * Записи, пропавшие из каталога, здесь не вычищаются: узнать о пропаже,
 * не перечислив каталог целиком, нельзя. Их подберёт ближайший полный обход,
 * а лишнее имя в файле безвредно: клиент спрашивает по номеру, а не читает
 * список подряд.
 */
export async function crawlPartial(seedRows, onFail) {
  const stat = newStat()
  const started = Date.now()

  const byId = new Map()
  for (const row of seedRows) byId.set(row.id, row)

  const ongoing = new Set()
  const anons = new Set()
  let added = 0
  let changed = 0

  function put(item) {
    const fresh = rowOf(item)
    const known = byId.get(item.id)
    if (known === undefined) added++
    else if (known.russian !== fresh.russian || known.aired_on !== fresh.aired_on) changed++
    byId.set(item.id, fresh)
  }

  // Хвост каталога. Страница считается по числу записей в семени: порядок
  // восходящий, значит новое лежит за ним.
  const tailFrom = Math.max(1, Math.floor(seedRows.length / BATCH) - TAIL_SLACK_PAGES)
  console.log(`Частичный обход: хвост каталога со страницы ${tailFrom}`)
  await sweep(stat, '', tailFrom, put, onFail)

  console.log('Частичный обход: идущее и анонсы')
  await sweep(
    stat,
    'status=ongoing,anons',
    1,
    (item) => {
      if (item.status === 'ongoing') ongoing.add(item.id)
      else if (item.status === 'anons') anons.add(item.id)
      put(item)
    },
    onFail,
  )

  // Порядок в файле восходящий, как после полного обхода: читателю файла
  // спокойнее, а разница между выпусками остаётся читаемой.
  const rows = [...byId.values()].sort((a, b) => a.id - b.id)

  stat.tookMs = Date.now() - started
  console.log(`Частичный обход: новых ${added}, обновлённых ${changed}`)

  return { stat, rows, ongoing, anons, mode: 'partial', added, changed }
}
