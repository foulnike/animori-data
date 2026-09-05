// Сборщик датасета русских названий (шаг 4 в docs/DATA-PIPELINE.md приложения).
//
// Перечисляет каталог Шикимори постранично и складывает рядом с собой три
// файла: имена, карту номеров и опись с отпечатками. Их подбирает шаг выпуска.
//
// После обхода идут две волны обогащения, обе необязательные: карта
// дополняется с AniList, пустые имена добираются с anime365. Волны стоят
// после порогов приёмки намеренно — сначала мы знаем, что сборка состоялась,
// и только потом тратим час на то, что улучшает её, но не решает судьбу.
//
// СЕМЯ КАРТЫ. Волна карты получает пары прошлого выпуска и спрашивает AniList
// только про новые номера. Следствий два. Обычная неделя стоит десятки
// запросов вместо шестисот десяти. И, что важнее, неделя, в которую AniList
// не отвечает вовсе, отдаёт в выпуск карту прошлой недели, а не пустой файл:
// до семени волна в такой неделе сворачивалась по пяти отказам, карта уходила
// пустой, и пороги приёмки этого не замечали — они стоят до волн.
// Плата за семя — карта умеет застывать незаметно, ровно как застыл бы вход
// на манами. Поэтому её возраст назван в описи, в самом файле карты
// и в описании выпуска, а порог приёмки у карты теперь свой.
//
// ПОЧЕМУ НЕ МАНАМИ. Раньше номера MyAnimeList брались из выпусков
// manami-project/anime-offline-database. 4 июля 2026 репозиторий переведён
// в архив: он доступен только для чтения, и новых недельных выпусков не будет.
// Вход конвейера навсегда замер бы на теге 2026-27, а пороги приёмки этого
// не заметили бы никогда: при замороженном входе они остаются зелёными вечно.
// Поэтому универсум номеров собирается сам, перечислением каталога.
//
// ЦЕНА ПЕРЕХОДА. Замер сентября 2026: каталог отдаёт 30 471 запись при
// limit=50, то есть 610 страниц против 612 запросов у прежнего обхода по
// явным ids. Обход не подорожал. Даром достались status и aired_on у каждой
// записи — на них считаются счётчики свежести для описи. Волна карты дорожала
// лишь однажды: пока семени не было, она спрашивала про все тридцать тысяч
// номеров. С семенем прошлого выпуска цена вернулась к прежней, и чужой
// архив для этого не понадобился.
//
// ЛОВУШКА ЦЕНЗУРЫ. Перечисление без censored=false отдаёт урезанный каталог
// и молча теряет около шести тысяч записей: цензурированный кончается между
// смещениями 23 000 и 24 990, а полный идёт до 30 471. Прежний обход по явным
// ids этот фильтр обходил, поэтому параметр и не был нужен. Убирать нельзя.
//
// Сам не публикует ничего и в репозиторий не пишет: публикация — отдельный
// шаг workflow, и он не выполнится, если сборка упала. Половина датасета
// хуже, чем его отсутствие, поэтому пороги приёмки проверяются здесь.
//
// Зависимостей нет намеренно: всё нужное есть в Node из коробки.

import { createHash } from 'node:crypto'
import { appendFileSync, writeFileSync } from 'node:fs'
import { gunzipSync, gzipSync } from 'node:zlib'

import { bump, pct, round1, sleep, why } from './common.mjs'
import { enrichMap } from './enrich-map.mjs'
import { enrichNames } from './enrich-names.mjs'

/**
 * Потолок Шикимори на одну страницу. limit=100 не ошибка, но и не работает:
 * замер показал, что он молча приводится к пятидесяти. Страница page=306
 * при limit=100 вернула данные со смещения 15 250, а не 30 500. Значит обход
 * вдвое не сократить, и 610 страниц — это пол, а не оценка.
 */
const BATCH = 50
/**
 * Адреса по порядку. Замер сентября 2026: rip не отказал ни разу из четырёх
 * проб и отдаёт настоящие пути постеров, io отказал один раз из шести,
 * one — дважды из четырёх. Поэтому rip теперь первый, а one остался
 * последним запасным ходом, а не первым выбором, как было раньше.
 *
 * Замер сделан с раннеров GitHub, то есть из-за границы. У клиента свой
 * порядок в SHIKI_DOMAINS и свои причины: там адрес российский, и смысл .rip
 * именно в обходе блокировки. Переносить этот порядок в клиент без замера
 * с клиентской стороны нельзя.
 */
const MIRRORS = ['shikimori.rip', 'shikimori.io', 'shikimori.one']
/** После скольких отказов подряд обход уходит на следующий адрес. */
const SWITCH_AFTER = 3
/** Осмысленный User-Agent обязателен: без него Шикимори отвечает отказом. */
const UA = 'AniMori/3.0 (+https://github.com/foulnike/animori-data)'
/** Адреса складываются из частей, как зеркала в api/shikimori.ts приложения. */
const SHIKI_PATH = '/api/animes'
/** Потолок одного запроса: виснувшее соединение не должно съесть весь прогон. */
const TIMEOUT_MS = 15000
/**
 * Предохранитель от бесконечного перечисления. Замер дал 610 страниц, потолок
 * page у Шикимори — сто тысяч, а отсечки по смещению нет вовсе: за концом
 * каталога приходит пустой массив, а не ошибка. Упёрлись в две тысячи страниц
 * (сто тысяч записей, втрое больше каталога) — сломалось условие остановки,
 * и сборка обязана упасть, а не крутиться до таймаута прогона.
 */
const MAX_PAGES = 2000
/**
 * Файлы прошлого выпуска: база сравнения для порогов и семя для волны карты.
 * Постоянный адрес, тот же, что читает клиент. Без токена: это раздача
 * выпусков, а не API GitHub, и лимита анонимных запросов здесь нет.
 */
const RELEASE_BASE = 'https://github.com/foulnike/animori-data/releases/latest/download'
/** Порог приёмки. Ниже — сборка не состоялась и наружу не выходит. */
const MIN_TITLES = 25000
/**
 * Насколько каталог вправе усохнуть против прошлого выпуска. Записи у Шикимори
 * изредка пропадают, и процент-два — обычная уборка. Обвал на двадцатую часть
 * означает не уборку, а поломку: оборванное перечисление, подмену ответа
 * или потерю censored=false. Прежний порог по доле узнанных номеров такое
 * не ловил и после перехода на перечисление стал тождественной единицей.
 *
 * Тем же порогом проверяется карта: с семенем она вообще не вправе стать
 * меньше прошлого выпуска, и просадка означает потерю семени, а не уборку.
 */
const MAX_SHRINK = 0.05
/** Окно свежести содержимого: записи, начавшие выходить за последние полгода. */
const FRESH_DAYS = 180
/** Через сколько страниц печатается строка о ходе дела. */
const REPORT_EVERY = 50
const CYRILLIC = /[А-Яа-яЁё]/

const PAUSE_MS = Number(process.env.BUILD_PAUSE || 700)

const FILE_TITLES = 'titles-anime.json.gz'
const FILE_MAP = 'map-mal-anilist.json.gz'
const FILE_INDEX = 'index.json'
const FILE_NOTES = 'release-notes.md'

/**
 * Падаем громко: тихий выход с нулём — это ложный зелёный прогон и, что хуже,
 * выпуск из пустоты. Причина едет и в итог прогона, а не только в лог шага.
 */
function fail(message) {
  console.error(`СБОРКА НЕ СОСТОЯЛАСЬ: ${message}`)
  if (process.env.GITHUB_STEP_SUMMARY) {
    const line = `**Сборка не состоялась:** ${message}\n`
    appendFileSync(process.env.GITHUB_STEP_SUMMARY, line, 'utf8')
  }
  process.exit(1)
}

/**
 * Опись прошлого выпуска. Отказ чтения не роняет сборку: первый прогон
 * в пустом репозитории базы сравнения не имеет, и это законно. Проверка
 * усадки тогда просто не делается, а порог по количеству остаётся.
 *
 * Отсюда же берётся строка о карте: по ней качается и сверяется семя.
 */
async function loadBaseline() {
  try {
    const answer = await fetch(`${RELEASE_BASE}/${FILE_INDEX}`, {
      headers: { 'user-agent': UA, accept: 'application/json' },
      signal: AbortSignal.timeout(TIMEOUT_MS),
    })

    if (!answer.ok) {
      console.log(`База сравнения: HTTP ${answer.status}, проверка усадки пропущена`)
      return null
    }

    const body = await answer.json()
    const count = body && body.names ? Number(body.names.count) : 0
    if (!Number.isFinite(count) || count <= 0) {
      console.log('База сравнения: в описи нет числа записей, проверка усадки пропущена')
      return null
    }

    const maxId = body && body.freshness ? Number(body.freshness.maxId) : 0
    // Строка о карте нужна целиком: в ней и имя файла, и отпечаток для сверки.
    const files = body && Array.isArray(body.files) ? body.files : []
    const mapFile = files.find((file) => file && file.name === FILE_MAP) || null

    console.log(`База сравнения: прошлый выпуск ${count} записей`)
    return {
      count,
      maxId: Number.isFinite(maxId) ? maxId : 0,
      tag: typeof body.sourceTag === 'string' ? body.sourceTag : '',
      builtAt: typeof body.builtAt === 'string' ? body.builtAt : '',
      mapFile,
    }
  } catch (e) {
    console.log(`База сравнения не скачалась (${why(e)}), проверка усадки пропущена`)
    return null
  }
}

/**
 * Карта прошлого выпуска — семя волны. Отказ не роняет сборку: без семени
 * волна спросит AniList про все номера, как делала до его появления.
 *
 * Отпечаток сверяется до распаковки, как это делает клиент в api/dataset.ts:
 * половина архива, разобранная в пары, хуже отсутствия семени. Пары приходят
 * из своего же выпуска, но проверяются как чужие: порченая пара тихо увела бы
 * клиента на чужой тайтл, и найти такое потом почти нельзя.
 */
async function loadSeed(mapFile) {
  if (mapFile === null) {
    console.log('Семя карты: в описи прошлого выпуска нет строки о карте')
    return null
  }

  try {
    const answer = await fetch(`${RELEASE_BASE}/${mapFile.name}`, {
      headers: { 'user-agent': UA },
      signal: AbortSignal.timeout(TIMEOUT_MS),
    })

    if (!answer.ok) {
      console.log(`Семя карты: HTTP ${answer.status}, волна спросит про все номера`)
      return null
    }

    const packed = Buffer.from(await answer.arrayBuffer())
    const digest = createHash('sha256').update(packed).digest('hex')
    const stamp = typeof mapFile.sha256 === 'string' ? mapFile.sha256 : ''
    if (stamp !== '' && digest !== stamp) {
      console.log('Семя карты: отпечаток не сошёлся с описью, семя отброшено')
      return null
    }

    const body = JSON.parse(gunzipSync(packed).toString('utf8'))
    const raw = body && Array.isArray(body.pairs) ? body.pairs : null
    if (raw === null) {
      console.log('Семя карты: в файле нет списка пар, семя отброшено')
      return null
    }

    const pairs = []
    for (const pair of raw) {
      if (!Array.isArray(pair) || pair.length !== 2) continue
      const [mal, anilist] = pair
      if (!Number.isInteger(mal) || !Number.isInteger(anilist)) continue
      if (mal <= 0 || anilist <= 0) continue
      pairs.push([mal, anilist])
    }

    const dropped = raw.length - pairs.length
    const tag = typeof body.tag === 'string' && body.tag !== '' ? body.tag : 'без тега'
    const tail = dropped > 0 ? `, отброшено порченых ${dropped}` : ''
    console.log(`Семя карты: ${pairs.length} пар от ${tag}${tail}`)

    return {
      pairs,
      tag: typeof body.tag === 'string' ? body.tag : '',
      builtAt: typeof body.builtAt === 'string' ? body.builtAt : '',
    }
  } catch (e) {
    console.log(`Семя карты не скачалось (${why(e)}), волна спросит про все номера`)
    return null
  }
}

/**
 * Одна страница каталога. Куки не шлём — клиент тоже не шлёт: с ними бывает 400.
 * order=id даёт устойчивый порядок, censored=false — полный каталог.
 */
async function ask(domain, page) {
  const url =
    'https://' +
    domain +
    SHIKI_PATH +
    `?page=${page}&limit=${BATCH}&order=id&censored=false`
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

/**
 * Полное перечисление каталога. Страница не бросается при отказе: она
 * повторяется, а после трёх отказов подряд обход уходит на следующий адрес.
 * Прогон, где первое зеркало молчало на каждой пачке, уже был на пробе —
 * сборка обязана это пережить.
 *
 * Заодно считаются счётчики свежести. Они берутся из status и aired_on,
 * которых в файле записей нет и не будет: описи они нужны, а клиенту нет.
 */
async function crawl() {
  const stat = {
    mirror: MIRRORS[0],
    requests: 0,
    pages: 0,
    codes: {},
    tookMs: 0,
  }
  const fresh = { maxId: 0, airedRecent: 0, released: 0, ongoing: 0, anons: 0 }
  const rows = []
  const seen = new Set()
  const startedAll = Date.now()

  // Границы окна свежести строками: обе даты в формате YYYY-MM-DD, и сравнение
  // строк для них совпадает со сравнением дат.
  const today = new Date().toISOString().slice(0, 10)
  const recentFrom = new Date(Date.now() - FRESH_DAYS * 86400000)
    .toISOString()
    .slice(0, 10)

  let page = 1
  let mirrorAt = 0
  let misses = 0

  for (;;) {
    if (page > MAX_PAGES) {
      fail(`перечисление не кончилось за ${MAX_PAGES} страниц: условие остановки сломано`)
    }

    const domain = MIRRORS[mirrorAt]
    let answer = await ask(domain, page)

    // Один повтор на 429: лимит говорит «подожди», а не «уходи».
    if (answer.status === 429) {
      const wait = Math.max(answer.retryAfter * 1000, 5000)
      console.log(`${domain}: 429, ждём ${wait} мс и повторяем ту же страницу`)
      stat.requests++
      bump(stat.codes, 429)
      await sleep(wait)
      answer = await ask(domain, page)
    }

    stat.requests++
    bump(stat.codes, answer.status)

    if (answer.status !== 200) {
      misses++
      console.log(`${domain}: страница ${page} — ${answer.status}`)

      if (misses >= SWITCH_AFTER) {
        mirrorAt++
        misses = 0
        if (mirrorAt >= MIRRORS.length) {
          fail(`все адреса Шикимори перестали отвечать на странице ${page}`)
        }
        stat.mirror = MIRRORS[mirrorAt]
        console.log(`Уходим на ${stat.mirror}: ${domain} не отвечает`)
      }

      await sleep(PAUSE_MS)
      continue
    }

    misses = 0

    // Пустая страница — законный конец каталога, а не отказ: отсечки
    // по смещению у Шикимори нет, за последней страницей приходит [].
    if (answer.items.length === 0) {
      console.log(`Каталог кончился: страница ${page} пуста`)
      break
    }

    stat.pages++

    for (const item of answer.items) {
      const id = item.id
      if (!Number.isFinite(id) || id <= 0) continue

      // Страницы могут перекрыться, если каталог пополнился прямо во время
      // обхода: order=id держит порядок, но новая запись сдвигает хвост.
      // Повтор отбрасывается по номеру, иначе он попал бы в файл дважды.
      if (seen.has(id)) continue
      seen.add(id)

      const russian = (item.russian || '').trim()

      if (id > fresh.maxId) fresh.maxId = id
      if (item.status === 'released') fresh.released++
      else if (item.status === 'ongoing') fresh.ongoing++
      else if (item.status === 'anons') fresh.anons++

      // aired_on бывает null и бывает в будущем: у анонсов там дата
      // следующего года, встречались 2026-10-04 и 2027-01-01. Поэтому окно
      // закрыто с двух сторон, а анонсы в счёт свежести не идут вовсе.
      // Из-за них максимум по aired_on негоден как метрика в принципе.
      const aired = typeof item.aired_on === 'string' ? item.aired_on : ''
      if (aired >= recentFrom && aired <= today && item.status !== 'anons') {
        fresh.airedRecent++
      }

      // Ровно шесть полей: чем меньше в файле, тем быстрее он грузится.
      rows.push({
        id,
        name: item.name,
        russian,
        kind: item.kind,
        aired_on: item.aired_on,
        score: item.score,
      })
    }

    if (stat.pages % REPORT_EVERY === 0) {
      console.log(`Обход: страница ${page}, записей ${rows.length}`)
    }

    page++
    await sleep(PAUSE_MS)
  }

  stat.tookMs = Date.now() - startedAll
  return { stat, rows, fresh }
}

/**
 * Пересчёт имён по готовым записям. Считается в конце, а не по ходу 