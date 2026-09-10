// Сборщик датасета русских названий (шаг 4 в docs/DATA-PIPELINE.md приложения).
//
// Здесь живут семена прошлого выпуска, выбор вида обхода, пороги приёмки
// и две волны обогащения. Перечисление каталога — в scripts/crawl.mjs,
// запись трёх файлов и отчёт — в scripts/release-files.mjs.
//
// Волны обе необязательные: карта дополняется с AniList, пустые имена
// добираются с anime365. Они стоят после порогов намеренно — сначала мы знаем,
// что сборка состоялась, и только потом тратим час на то, что улучшает её,
// но не решает судьбу.
//
// ВИД ОБХОДА. Обычная неделя идёт частичным обходом: семенем берётся файл
// имён прошлого выпуска, а у Шикимори спрашивается только то, что впрямь
// могло измениться: хвост за прошлой головой и весь список идущего
// и анонсов. Полный обход случается, когда наследовать нечего (первый прогон,
// потеря или порча семени) или со времени прошлого полного вышло больше
// FULL_EVERY_DAYS дней: редкие правки названий у вышедшего частичный обход
// не видит, и копиться им вечно нельзя. Отметка последнего полного живёт
// в самой описи (names.fullAt), поэтому ни состояния в репозитории, ни памяти
// между прогонами не требуется. BUILD_MODE=full и partial говорят прямо.
//
// СЕМЯ ИМЁН. Файл прошлого выпуска качается с постоянного адреса, отпечаток
// сверяется до распаковки, как делает клиент в api/dataset.ts. Порченое или
// подозрительно маленькое семя отбрасывается целиком, и обход становится
// полным: половина каталога хуже, чем лишние десять минут сети.
//
// СЕМЯ КАРТЫ. Волна карты получает пары прошлого выпуска и спрашивает AniList
// только про новые номера. Следствий два. Обычная неделя стоит десятки
// запросов вместо шестисот десяти. И, что важнее, неделя, в которую AniList
// не отвечает вовсе, отдаёт в выпуск карту прошлой недели, а не пустой файл.
// Плата за семя — карта умеет застывать незаметно, ровно как застыл бы вход
// на манами. Поэтому её возраст назван в описи, в самом файле карты и в
// описании выпуска, а порог приёмки у карты свой.
//
// ПОЧЕМУ НЕ МАНАМИ. Раньше номера MyAnimeList брались из выпусков
// manami-project/anime-offline-database. 4 июля 2026 репозиторий переведён в архив:
// новых недельных выпусков не будет. Вход конвейера навсегда замер бы на
// теге 2026-27, а пороги приёмки этого не заметили бы никогда: при замороженном
// входе они остаются зелёными вечно. Поэтому универсум номеров собирается сам.
//
// Сам не публикует ничего и в репозиторий не пишет: публикация — отдельный
// шаг workflow, и он не выполнится, если сборка упала. Половина датасета
// хуже, чем его отсутствие, поэтому пороги приёмки проверяются здесь.
//
// Зависимостей нет намеренно: всё нужное есть в Node из коробки.

import { createHash } from 'node:crypto'
import { appendFileSync } from 'node:fs'
import { gunzipSync } from 'node:zlib'

import { why } from './common.mjs'
import { TIMEOUT_MS, UA, crawlFull, crawlPartial } from './crawl.mjs'
import { enrichMap } from './enrich-map.mjs'
import { enrichNames } from './enrich-names.mjs'
import { FILE_INDEX, FILE_MAP, FILE_TITLES, writeRelease } from './release-files.mjs'

/**
 * Файлы прошлого выпуска: база сравнения для порогов и семена для обхода
 * и волны карты. Постоянный адрес, тот же, что читает клиент. Без токена:
 * это раздача выпусков, а не API GitHub, и лимита анонимных запросов здесь нет.
 */
const RELEASE_BASE = 'https://github.com/foulnike/animori-data/releases/latest/download'
/** Порог приёмки. Ниже — сборка не состоялась и наружу не выходит. */
const MIN_TITLES = 25000
/**
 * Насколько каталог вправе усохнуть против прошлого выпуска. Записи у Шикимори
 * изредка пропадают, и процент-два — обычная уборка. Обвал на двадцатую часть
 * означает не уборку, а поломку: оборванное перечисление, подмену ответа
 * или потерю censored=false.
 *
 * Тем же порогом проверяется карта: с семенем она вообще не вправе стать
 * меньше прошлого выпуска, и просадка означает потерю семени, а не уборку.
 */
const MAX_SHRINK = 0.05
/** Окно свежести содержимого: записи, начавшие выходить за последние полгода. */
const FRESH_DAYS = 180
/**
 * Через сколько дней частичные обходы обязаны уступить полному. Тридцать —
 * это четыре-пять недельных прогонов подряд.
 */
const FULL_EVERY_DAYS = 30
const DAY_MS = 86400000
const CYRILLIC = /[А-Яа-яЁё]/

const PAUSE_MS = Number(process.env.BUILD_PAUSE || 700)
/** auto — решает отметка полного обхода в описи; full и partial говорят прямо. */
const MODE = String(process.env.BUILD_MODE || 'auto').toLowerCase()

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

/** Строка описи по имени файла: в ней и адрес, и отпечаток для сверки. */
function fileRef(files, name) {
  const list = Array.isArray(files) ? files : []
  return list.find((file) => file && file.name === name) || null
}

/**
 * Опись прошлого выпуска. Отказ чтения не роняет сборку: первый прогон
 * в пустом репозитории базы сравнения не имеет, и это законно. Проверка
 * усадки тогда не делается, порог по количеству остаётся, а обход идёт
 * полный — наследовать нечего.
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
    const files = body && Array.isArray(body.files) ? body.files : []

    console.log(`База сравнения: прошлый выпуск ${count} записей`)
    return {
      count,
      maxId: Number.isFinite(maxId) ? maxId : 0,
      tag: typeof body.sourceTag === 'string' ? body.sourceTag : '',
      builtAt: typeof body.builtAt === 'string' ? body.builtAt : '',
      // Отметка последнего полного обхода. У выпусков до этой правки её нет,
      // и это ровно тот случай, когда обход обязан быть полным.
      fullAt: body && body.names && typeof body.names.fullAt === 'string' ? body.names.fullAt : '',
      titlesFile: fileRef(files, FILE_TITLES),
      mapFile: fileRef(files, FILE_MAP),
    }
  } catch (e) {
    console.log(`База сравнения не скачалась (${why(e)}), проверка усадки пропущена`)
    return null
  }
}

/**
 * Скачивает сжатый файл выпуска и сверяет отпечаток до распаковки — так же,
 * как это делает клиент в api/dataset.ts. Файлы приходят из своего же выпуска,
 * но проверяются как чужие: половина архива, разобранная в данные, хуже
 * отсутствия семени, а порченая пара тихо увела бы клиента на чужой тайтл.
 */
async function loadPacked(ref, what) {
  if (ref === null) {
    console.log(`${what}: в описи прошлого выпуска нет строки о файле`)
    return null
  }

  const answer = await fetch(`${RELEASE_BASE}/${ref.name}`, {
    headers: { 'user-agent': UA },
    signal: AbortSignal.timeout(TIMEOUT_MS),
  })

  if (!answer.ok) {
    console.log(`${what}: HTTP ${answer.status}`)
    return null
  }

  const packed = Buffer.from(await answer.arrayBuffer())
  const digest = createHash('sha256').update(packed).digest('hex')
  const stamp = typeof ref.sha256 === 'string' ? ref.sha256 : ''
  if (stamp !== '' && digest !== stamp) {
    console.log(`${what}: отпечаток не сошёлся с описью, файл отброшен`)
    return null
  }

  return JSON.parse(gunzipSync(packed).toString('utf8'))
}

/**
 * Имена прошлого выпуска — семя частичного обхода. Отказ не роняет сборку:
 * без семени обход просто идёт полным, как шла каждая неделя раньше.
 *
 * Строки проверяются по одной: номер обязан быть целым и положительным,
 * остальные поля приводятся к тому виду, в каком их пишет обход. Семя меньше
 * порога приёмки отбрасывается целиком: добирать хвост к половине каталога
 * значило бы выпустить половину и не заметить этого.
 */
async function loadSeedTitles(ref) {
  try {
    const body = await loadPacked(ref, 'Семя имён')
    if (body === null) return null

    const raw = Array.isArray(body.titles) ? body.titles : null
    if (raw === null) {
      console.log('Семя имён: в файле нет списка записей, семя отброшено')
      return null
    }

    const rows = []
    for (const row of raw) {
      if (!row || !Number.isInteger(row.id) || row.id <= 0) continue
      rows.push({
        id: row.id,
        name: typeof row.name === 'string' ? row.name : '',
        russian: typeof row.russian === 'string' ? row.russian.trim() : '',
        kind: typeof row.kind === 'string' ? row.kind : null,
        aired_on: typeof row.aired_on === 'string' ? row.aired_on : null,
        score: row.score === undefined ? null : row.score,
      })
    }

    if (rows.length < MIN_TITLES) {
      console.log(`Семя имён: в файле всего ${rows.length} записей, семя отброшено`)
      return null
    }

    const dropped = raw.length - rows.length
    const tag = typeof body.tag === 'string' && body.tag !== '' ? body.tag : 'без тега'
    const tail = dropped > 0 ? `, отброшено порченых ${dropped}` : ''
    console.log(`Семя имён: ${rows.length} записей от ${tag}${tail}`)

    return { rows, tag: typeof body.tag === 'string' ? body.tag : '' }
  } catch (e) {
    console.log(`Семя имён не скачалось (${why(e)}), обход пойдёт полным`)
    return null
  }
}

/**
 * Карта прошлого выпуска — семя волны AniList. Отказ не роняет сборку: без
 * семени волна спросит про все номера, как делала до его появления.
 */
async function loadSeed(mapFile) {
  try {
    const body = await loadPacked(mapFile, 'Семя карты')
    if (body === null) return null

    const raw = Array.isArray(body.pairs) ? body.pairs : null
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
 * Вид обхода. Настройка говорит прямо; в auto решает отметка последнего полного
 * обхода в описи. Нет семени — выбора нет вовсе, даже если просили partial:
 * добирать хвост не к чему.
 */
function wantsFull(baseline, seedTitles) {
  if (seedTitles === null || baseline === null) {
    console.log('Вид обхода: полный, наследовать нечего')
    return true
  }

  if (MODE === 'full') {
    console.log('Вид обхода: полный по настройке')
    return true
  }

  if (MODE === 'partial') {
    console.log('Вид обхода: частичный по настройке')
    return false
  }

  if (baseline.fullAt === '') {
    console.log('Вид обхода: полный, в прошлой описи нет отметки полного обхода')
    return true
  }

  const was = Date.parse(baseline.fullAt)
  if (!Number.isFinite(was)) {
    console.log('Вид обхода: полный, отметка полного обхода не разобралась')
    return true
  }

  const days = Math.floor((Date.now() - was) / DAY_MS)
  if (days >= FULL_EVERY_DAYS) {
    console.log(`Вид обхода: полный, со времени прошлого полного ${days} дн.`)
    return true
  }

  console.log(`Вид обхода: частичный, полный был ${days} дн. назад`)
  return false
}

/**
 * Пересчёт имён по готовым записям. Считается в конце, а не по ходу обхода:
 * после волны anime365 счётчики обхода уже устарели, а опись обязана
 * описывать то, что лежит в файле, а не то, что было в середине сборки.
 */
function countNames(rows) {
  let russian = 0
  let cyrillic = 0

  for (const row of rows) {
    if (row.russian !== '') russian++
    if (CYRILLIC.test(row.russian)) cyrillic++
  }

  return { russian, cyrillic }
}

/**
 * Свежесть содержимого, а не файла. Возраст выпуска сторож видит и без нас,
 * а вот застывший вход виден только отсюда.
 *
 * Состояний ровно три, и идущее с анонсами перечисляются целиком при любом
 * виде обхода — значит вышедшее есть весь остаток каталога, и наследовать
 * числа из прошлой описи не приходится ни разу. Это важно: унаследованное
 * число молча застыло бы, а сторож смотрит именно на них.
 */
function countFreshness(rows, ongoing, anons) {
  // Границы окна строками: обе даты в формате YYYY-MM-DD, и сравнение строк
  // для них совпадает со сравнением дат.
  const today = new Date().toISOString().slice(0, 10)
  const recentFrom = new Date(Date.now() - FRESH_DAYS * DAY_MS).toISOString().slice(0, 10)

  let maxId = 0
  let airedRecent = 0

  for (const row of rows) {
    if (row.id > maxId) maxId = row.id

    // aired_on бывает null и бывает в будущем: у анонсов там дата следующего
    // года, встречались 2026-10-04 и 2027-01-01. Поэтому окно закрыто с двух
    // сторон, а анонсы в счёт свежести не идут вовсе.
    if (anons.has(row.id)) continue
    const aired = typeof row.aired_on === 'string' ? row.aired_on : ''
    if (aired >= recentFrom && aired <= today) airedRecent++
  }

  return {
    maxId,
    freshDays: FRESH_DAYS,
    airedRecent,
    released: rows.length - ongoing.size - anons.size,
    ongoing: ongoing.size,
    anons: anons.size,
  }
}

async function main() {
  console.log(`Сборка: пауза ${PAUSE_MS} мс, каталог Шикимори`)

  const baseline = await loadBaseline()
  // Семя имён качается до обхода: от него зависит сам вид обхода.
  const seedTitles = baseline === null ? null : await loadSeedTitles(baseline.titlesFile)
  const full = wantsFull(baseline, seedTitles)

  const crawled = full ? await crawlFull(fail) : await crawlPartial(seedTitles.rows, fail)
  const { stat, rows, ongoing, anons, mode, added, changed } = crawled

  // Пороги проверяются до волн: сначала убеждаемся, что сборка состоялась,
  // и только потом тратим час на то, что делает её лучше.
  if (rows.length < MIN_TITLES) {
    fail(`собрано ${rows.length} названий при пороге ${MIN_TITLES}`)
  }

  const fresh = countFreshness(rows, ongoing, anons)

  if (baseline !== null) {
    const floor = Math.floor(baseline.count * (1 - MAX_SHRINK))
    if (rows.length < floor) {
      fail(
        `каталог усох: ${rows.length} записей против ${baseline.count} ` +
          `в прошлом выпуске, порог ${floor}`,
      )
    }
    if (fresh.maxId < baseline.maxId) {
      fail(
        `голова каталога уехала назад: ${fresh.maxId} против ${baseline.maxId} ` +
          'в прошлом выпуске',
      )
    }
  }

  const fromShiki = countNames(rows)
  const malIds = rows.map((row) => row.id)

  // Семя карты качается после порогов: если сборка не состоялась, тратить сеть
  // на карту прошлого выпуска незачем.
  const seed = baseline === null ? null : await loadSeed(baseline.mapFile)

  const map = await enrichMap(malIds, seed === null ? [] : seed.pairs)
  const extra = await enrichNames(rows)

  for (const row of rows) {
    if (row.russian !== '') continue
    const found = extra.names.get(row.id)
    if (found) row.russian = found
  }

  const pairs = map.pairs
  const total = countNames(rows)

  // Порог приёмки карты. У имён он был с самого начала, у карты не было вовсе —
  // и это ровно та щель, через которую пустая карта уезжала бы в выпуск при
  // зелёном прогоне. Первый прогон в пустом репозитории освобождён, как и
  // проверка усадки выше: наследовать ему нечего.
  if (baseline !== null) {
    if (seed !== null) {
      const floor = Math.floor(seed.pairs.length * (1 - MAX_SHRINK))
      if (pairs.length < floor) {
        fail(
          `карта усохла: ${pairs.length} пар против ${seed.pairs.length} ` +
            `в семени, порог ${floor}`,
        )
      }
    } else if (pairs.length === 0) {
      fail('карта пуста: семя не скачалось, а волна ничего не добрала')
    }
  }

  const builtAt = new Date().toISOString()
  // Тег свой, а не недельный тег манами: он называет голову каталога,
  // которую видела эта сборка. По движению тега видно, что вход живой.
  const sourceTag = `id-${fresh.maxId}`
  // Отметка полного обхода передаётся из выпуска в выпуск: именно она решает
  // вид следующего обхода, и другого места для неё нет.
  const fullAt = full ? builtAt : baseline.fullAt

  // Возраст карты. Свёрнутая или выключенная волна не добавляет ни одной пары —
  // тогда карта в выпуске тождественна семени, и называть её сегодняшней было бы
  // ложью. Отличать это от «волна отработала, новых номеров не нашлось»
  // обязательно: во втором случае карта именно сегодняшняя, и added там ноль
  // по совершенно другой причине.
  const mapStalled = map.stat.added === 0 && (map.stat.gaveUp || map.stat.skipped)
  const mapAged = mapStalled && seed !== null
  const mapFrom = mapAged ? seed.tag : sourceTag
  const mapBuiltAt = mapAged ? seed.builtAt : builtAt
  const mapWave = map.stat.skipped
    ? 'выключена'
    : map.stat.disabled
      ? 'доступ закрыт'
      : map.stat.gaveUp
        ? 'свёрнута'
        : 'отработала'

  writeRelease({
    rows,
    pairs,
    fresh,
    total,
    fromShiki,
    stat,
    map,
    extra,
    builtAt,
    sourceTag,
    fullAt,
    mode,
    added,
    changed,
    mapFrom,
    mapBuiltAt,
    mapAged,
    mapWave,
    baseline,
    seed,
    seedTitles,
  })
}

await main()
