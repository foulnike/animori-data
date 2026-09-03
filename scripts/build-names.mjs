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
// записи — на них считаются счётчики свежести для описи. Подорожала только
// волна карты: манами отдавала 18 858 пар бесплатно, а теперь про все номера
// приходится спрашивать AniList.
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
import { gzipSync } from 'node:zlib'

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
 * Опись прошлого выпуска: единственная живая база сравнения. Постоянный адрес,
 * тот же, что читает клиент. Без токена: репозиторий публичный.
 */
const LATEST_INDEX =
  'https://github.com/foulnike/animori-data/releases/latest/download/index.json'
/** Порог приёмки. Ниже — сборка не состоялась и наружу не выходит. */
const MIN_TITLES = 25000
/**
 * Насколько каталог вправе усохнуть против прошлого выпуска. Записи у Шикимори
 * изредка пропадают, и процент-два — обычная уборка. Обвал на двадцатую часть
 * означает не уборку, а поломку: оборванное перечисление, подмену ответа
 * или потерю censored=false. Прежний порог по доле узнанных номеров такое
 * не ловил и после перехода на перечисление стал тождественной единицей.
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
 */
async function loadBaseline() {
  try {
    const answer = await fetch(LATEST_INDEX, {
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
    console.log(`База сравнения: прошлый выпуск ${count} записей`)
    return { count, maxId: Number.isFinite(maxId) ? maxId : 0 }
  } catch (e) {
    console.log(`База сравнения не скачалась (${why(e)}), проверка усадки пропущена`)
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

/** Пишет сжатый файл и возвращает строку описи: имя, размер, отпечаток. */
function pack(name, payload) {
  const body = gzipSync(Buffer.from(JSON.stringify(payload), 'utf8'), { level: 9 })
  writeFileSync(name, body)
  return {
    name,
    bytes: body.length,
    sha256: createHash('sha256').update(body).digest('hex'),
  }
}

/** Печатает и в лог, и в итог прогона: за числами не надо лезть в артефакт. */
function report(lines) {
  const text = lines.join('\n')
  console.log(`\n${text}\n`)
  if (process.env.GITHUB_STEP_SUMMARY) {
    appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${text}\n`, 'utf8')
  }
}

/** Строка отчёта о волне: пропущена, свёрнута или отработала целиком. */
function waveLine(stat, done) {
  if (stat.skipped) return 'выключена настройкой'
  const tail = stat.gaveUp ? ', волна свёрнута досрочно' : ''
  return `${done} за ${round1(stat.tookMs / 60000)} мин${tail}`
}

async function main() {
  console.log(`Сборка: пауза ${PAUSE_MS} мс, перечисление каталога Шикимори`)

  const baseline = await loadBaseline()
  const { stat, rows, fresh } = await crawl()

  // Пороги проверяются до волн: сначала убеждаемся, что сборка состоялась,
  // и только потом тратим час на то, что делает её лучше.
  if (rows.length < MIN_TITLES) {
    fail(`собрано ${rows.length} названий при пороге ${MIN_TITLES}`)
  }

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

  // Готовых пар больше нет: манами отдавала 18 858 бесплатно, теперь волна
  // спрашивает AniList про все номера. Пустой список означает ровно это.
  const map = await enrichMap(malIds, [])
  const extra = await enrichNames(rows)

  for (const row of rows) {
    if (row.russian !== '') continue
    const found = extra.names.get(row.id)
    if (found) row.russian = found
  }

  const pairs = map.pairs
  const total = countNames(rows)

  const builtAt = new Date().toISOString()
  // Тег теперь свой, а не недельный тег манами: он называет голову каталога,
  // которую видела эта сборка. По движению тега видно, что вход живой.
  const sourceTag = `id-${fresh.maxId}`
  const head = { v: 1, tag: sourceTag, builtAt }

  const titlesFile = pack(FILE_TITLES, { ...head, count: rows.length, titles: rows })
  const mapFile = pack(FILE_MAP, { ...head, count: pairs.length, pairs })

  // Версия описи остаётся первой: поля только добавляются, и старый клиент
  // читает её как прежде. Поле count как было числом записей, так и осталось —
  // менять смысл имеющегося поля значило бы соврать всем, кто уже его читает.
  //
  // names.known остаётся ради совместимости, но смысла в нём больше нет:
  // при перечислении мы получаем ровно то, что каталог отдал, и доля узнанных
  // номеров тождественно равна единице. Живую проверку делает сравнение
  // с прошлым выпуском выше, а не этот порог.
  //
  // license — CC0-1.0, полный отказ от прав. Путь был такой: ODbL-1.0 стояла
  // не по выбору, а приезжала вместе с производностью от манами; производности
  // больше нет, а ни Шикимори, ни anime365, ни AniList условий на выгрузку
  // через открытый API не налагают. Коротко стояла MIT — и тоже не к месту:
  // это лицензия для кода, и требование возить её текст в копиях сводки
  // номеров и названий — пустая формальность. CC0 говорит прямо про базы
  // данных и права на извлечение данных — ровно про то, чем этот файл и является.
  const index = {
    version: 1,
    builtAt,
    source: 'shikimori',
    sourceTag,
    license: 'CC0-1.0',
    names: {
      source: stat.mirror,
      count: rows.length,
      russian: total.russian,
      cyrillic: total.cyrillic,
      known: 1,
    },
    // Свежесть содержимого, а не файла. Возраст выпуска сторож видит и без нас,
    // а вот застывший вход виден только отсюда: голова каталога и число
    // записей, начавших выходить за последние FRESH_DAYS дней. Сторож сравнивает
    // maxId с текущей головой Шикимори и ловит застой, при котором выпуски
    // выходят исправно, а содержимое в них не меняется.
    freshness: {
      maxId: fresh.maxId,
      freshDays: FRESH_DAYS,
      airedRecent: fresh.airedRecent,
      released: fresh.released,
      ongoing: fresh.ongoing,
      anons: fresh.anons,
    },
    files: [titlesFile, mapFile],
  }
  writeFileSync(FILE_INDEX, `${JSON.stringify(index, null, 2)}\n`, 'utf8')

  // Источник и лицензия называются в описании выпуска намеренно: тот, кто
  // скачал файлы напрямую, за условиями в репозиторий не пойдёт. Атрибуция
  // манами убрана вместе с самим манами, а вместе с ней ушла и ODbL-1.0:
  // производной базы больше нет, и держать чужие условия не за что.
  writeFileSync(
    FILE_NOTES,
    [
      'Русские названия аниме для AniMori.',
      '',
      `Записей: ${rows.length}, с русским названием: ${total.russian}, ` +
        `из них кириллицей: ${total.cyrillic}.`,
      `Соответствий MAL — AniList: ${pairs.length}.`,
      `Собрано ${builtAt} через ${stat.mirror}.`,
      `Голова каталога — номер ${fresh.maxId}, за последние ${FRESH_DAYS} дней ` +
        `начали выходить ${fresh.airedRecent} записей.`,
      '',
      'Номера и русские названия получены перечислением открытого API Шикимори',
      'с параметром censored=false. Пары MAL — AniList дополнены с AniList,',
      'пустые названия добраны с anime365.',
      'Датасет выходит без прав и без условий: CC0-1.0, общественное достояние.',
      'Пользуйтесь как угодно, спроса нет.',
      '',
      'Постоянный адрес описи:',
      'https://github.com/foulnike/animori-data/releases/latest/download/index.json',
      '',
    ].join('\n'),
    'utf8',
  )

  const titlesMb = round1(titlesFile.bytes / 1048576)
  const mapMb = round1(mapFile.bytes / 1048576)
  const codes = Object.entries(stat.codes)
    .map(([code, count]) => `${code} × ${count}`)
    .join(', ')
  const baseLine =
    baseline === null ? 'нет базы сравнения' : `${baseline.count} записей`

  report([
    `## Сборка датасета: ${sourceTag}`,
    '',
    '| Что | Сколько |',
    '| --- | --- |',
    `| Страниц каталога | ${stat.pages} |`,
    `| Записей собрано | ${rows.length} |`,
    `| Голова каталога | ${fresh.maxId} |`,
    `| Прошлый выпуск | ${baseLine} |`,
    `| Русское имя от Шикимори | ${fromShiki.russian} |`,
    `| Добрано с anime365 | ${extra.stat.added} из ${extra.stat.empty} пустых |`,
    `| Русское имя всего | ${total.russian} (${pct(total.russian / rows.length)}) |`,
    `| Из них кириллицей | ${total.cyrillic} |`,
    `| Пары добраны с AniList | ${map.stat.added} |`,
    `| Пары всего | ${pairs.length} (${pct(pairs.length / rows.length)} от записей) |`,
    `| Вышло за ${FRESH_DAYS} дн. | ${fresh.airedRecent} |`,
    `| Состояния | released ${fresh.released}, ongoing ${fresh.ongoing}, anons ${fresh.anons} |`,
    `| Запросов к Шикимори | ${stat.requests}, ответы: ${codes} |`,
    `| Время обхода | ${round1(stat.tookMs / 60000)} мин через ${stat.mirror} |`,
    `| Волна карты | ${waveLine(map.stat, `${map.stat.added} пар`)} |`,
    `| Волна имён | ${waveLine(extra.stat, `${extra.stat.added} имён, ${extra.stat.latin} отброшено латиницей`)} |`,
    `| ${FILE_TITLES} | ${titlesMb} МБ |`,
    `| ${FILE_MAP} | ${mapMb} МБ |`,
    '',
    '**Файлы собраны. Публикация — следующим шагом, если она включена.**',
  ])
}

await main()
