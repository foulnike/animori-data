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
// ПЕРЕСБОРКА КАРТЫ. Семя переносится из выпуска в выпуск, и про уже
// известные номера волна не переспрашивает никогда: ядро карты не
// обновляется само ни одного раза. Оттого в нём живут пары, доставшиеся
// от манами, и медленно копятся мёртвые: на AniList записи сливают
// и удаляют, а число пар при этом только растёт, и заметить порчу нечем.
// BUILD_MAP_SEED=off лечит это по просьбе: волна переспрашивает все номера
// до одного.
//
// Семя при этом в волну идёт, а не отменяется, и это главная разница против
// прежнего поведения. Пара уходит из карты только по успешному ответу AniList
// без неё; номер, до которого волна не дошла, остаётся как в семени. Значит
// оборванная пересборка — это пересборка, сделанная наполовину, а не потеря
// карты: порог приёмки она не роняет, и продолжить её можно на следующей
// неделе. Прежде исходов было ровно два, полная удача или полная потеря,
// и второй случался каждый раз. Стоит пересборка шестьсот десять запросов
// и больше двух часов, поэтому расписанию такое ни к чему — только кнопке.
//
// СРОК ПРОГОНА, А НЕ ПОТОЛОК ЗАДАНИЯ. У задания в workflow есть потолок
// времени, и отмена по нему убивает всё разом: и обход, и волны, и файлы,
// которых никто не успел записать. Именно так кончился живой прогон
// 13 сентября 2026 — отмена на семьдесят первом проценте волны карты,
// три часа работы в ничто.
//
// Поэтому сборщик знает свой срок (RUN_BUDGET) и держит его заведомо ниже
// потолка задания, а волнам выдаёт сроки от него: волна карты обязана
// вернуть управление к MAP_BUDGET, волна имён — к остатку, и у выпуска
// всегда остаётся запас на пересчёт, упаковку и запись. Сорванный срок —
// обычный исход волны, а не авария: она говорит об этом словами и отдаёт
// собранное.
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

import { round1, why } from './common.mjs'
import { TIMEOUT_MS, UA, crawlFull, crawlPartial } from './crawl.mjs'
import { enrichMap } from './enrich-map.mjs'
import { NAMES_BUDGET_MS, enrichNames } from './enrich-names.mjs'
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
 * Пересборка карты по просьбе: BUILD_MAP_SEED=off заставляет волну переспросить
 * все номера до одного. Семя при этом остаётся опорой волны и базой порога
 * приёмки. Нужна затем, что иначе ядро карты не обновляется никогда,
 * и в нём копятся пары умерших записей и остаток чужого происхождения.
 *
 * Запасное значение — семя на месте: пустая переменная у расписания не вправе
 * превратить недельный прогон в шестьсот десять запросов.
 */
const MAP_REBUILD = String(process.env.BUILD_MAP_SEED || 'on').toLowerCase() === 'off'

/** Когда прогон начался: от этой точки считаются сроки волн. */
const STARTED_AT = Date.now()
/**
 * Сколько всего минут отведено прогону. Число обязано быть заведомо ниже
 * потолка задания в workflow: отмена по потолку убивает файлы и выпуск,
 * а собственный срок всего лишь свёртывает волну и отдаёт собранное.
 */
const RUN_BUDGET_MS = Number(process.env.RUN_BUDGET || 300) * 60000
/**
 * Сколько из них позволено волне карты. Полная пересборка при паузе 2500 мс
 * стоит около двух с половиной часов, и полтора часа — это её половина:
 * дальше волна свернётся сама, а остаток доспросится на следующей неделе.
 */
const MAP_BUDGET_MS = Number(process.env.MAP_BUDGET || 150) * 60000
/** Запас на пересчёт имён, упаковку и запись трёх файлов. */
const RELEASE_RESERVE_MS = 3 * 60000

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
 *
 * Качается и при пересборке: там семя работает опорой, на которую ложатся
 * переспрошенные пары, и базой порога приёмки. Пересборка — ровно тот случай,
 * где оно нужнее всего.
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

  // План времени. Обе волны умеют не успеть, и сроки им считаются от начала
  // прогона, а не от их собственного старта: обход к этому времени уже съел
  // свою долю общего потолка. Запас выпуска вычитается первым — файлы, которых
  // никто не записал, обесценивают весь прогон.
  const runDeadline = STARTED_AT + RUN_BUDGET_MS
  const namesShare = process.env.BUILD_ANIME365 === 'off' ? 0 : NAMES_BUDGET_MS
  const namesDeadline = runDeadline - RELEASE_RESERVE_MS
  const mapDeadline = Math.min(Date.now() + MAP_BUDGET_MS, namesDeadline - namesShare)
  console.log(
    `План времени: прогон до ${round1(RUN_BUDGET_MS / 60000)} мин, ` +
      `карте ${round1(Math.max(0, mapDeadline - Date.now()) / 60000)} мин, ` +
      `именам ${round1(namesShare / 60000)} мин, ` +
      `выпуску ${round1(RELEASE_RESERVE_MS / 60000)} мин`,
  )

  // Пересборка без волны — это просьба, отменяющая сама себя: переспрашивать
  // нечем. Разбирается здесь, чтобы в волну не ушла настройка, которой она
  // всё равно не сможет подчиниться.
  const rebuild = MAP_REBUILD && process.env.BUILD_ANILIST !== 'off'
  if (MAP_REBUILD && !rebuild) {
    console.log('Карта: пересборка просилась, но волна AniList выключена — семя остаётся')
  } else if (rebuild) {
    console.log('Карта: пересборка по настройке, все номера переспрашиваются, семя остаётся опорой')
  }

  // Семя отдаётся волне всегда, в том числе в пересборку: там оно опора,
  // а не список пропусков. Пара уходит из карты только по успешному ответу
  // AniList без неё, а номер, до которого волна не дошла, остаётся как в семени.
  const map = await enrichMap(malIds, seed === null ? [] : seed.pairs, {
    deadline: mapDeadline,
    rebuild,
  })
  const extra = await enrichNames(rows, namesDeadline)

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
  //
  // Пересборку он больше не задевает: семя идёт в волну опорой, и оборванная
  // пересборка отдаёт семя плюс переспрошенное. Считается при этом не голое
  // число пар, а число вместе с доказанными покойниками: пару убирает только
  // успешный ответ AniList без неё, и честная уборка не обязана выглядеть
  // усадкой — иначе пересборка была бы запрещена насовсем.
  if (baseline !== null) {
    if (seed !== null) {
      const floor = Math.floor(seed.pairs.length * (1 - MAX_SHRINK))
      if (pairs.length + map.stat.dropped < floor) {
        fail(
          `карта усохла: ${pairs.length} пар против ${seed.pairs.length} ` +
            `в семени, порог ${floor} (убрано по ответу AniList ${map.stat.dropped})`,
        )
      }
      if (map.stat.dropped > 0) {
        console.log(
          `Карта: убрано ${map.stat.dropped} пар, отсутствие которых AniList подтвердил ответом`,
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

  // Возраст карты. Свёрнутая или выключенная волна не меняет в карте ничего —
  // тогда карта в выпуске тождественна семени, и называть её сегодняшней было бы
  // ложью. Отличать это от «волна отработала, новых номеров не нашлось»
  // обязательно: во втором случае карта именно сегодняшняя, и правок там ноль
  // по совершенно другой причине.
  //
  // Правкой считается любая: добор, переезд пары на другой номер и уборка
  // покойника. Оборванная пересборка, успевшая хоть что-то переспросить,
  // застывшей не считается — она и не застыла.
  const mapTouched = map.stat.added + map.stat.changed + map.stat.dropped > 0
  const mapStalled = !mapTouched && (map.stat.gaveUp || map.stat.skipped)
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

  // Полной пересборкой в описи и в описании выпуска называется только та,
  // что дошла до конца. Недошедшая — обычная волна, опёршаяся на семя,
  // и обещать в заметках выпуска большее нельзя: через полгода по этой
  // строке будут судить, когда ядро карты обновлялось целиком.
  if (rebuild && map.stat.gaveUp) {
    console.log(
      `Карта: пересборка не дошла до конца (переспрошено ${map.stat.asked}, ` +
        `оставлено от семени ${map.stat.kept}), в выпуске она названа обычной волной`,
    )
  }

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
    mapRebuild: rebuild && !map.stat.gaveUp,
    baseline,
    seed,
    seedTitles,
  })
}

await main()
