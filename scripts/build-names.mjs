// Сборщик датасета русских названий (шаг 4 в docs/DATA-PIPELINE.md приложения).
//
// Берёт номера MyAnimeList из последнего выпуска манами, обходит Шикимори
// пачками по пятьдесят и складывает рядом с собой три файла: имена, карту
// номеров и опись с отпечатками. Их подбирает шаг выпуска.
//
// Сам не публикует ничего и в репозиторий не пишет: публикация — отдельный
// шаг workflow, и он не выполнится, если сборка упала. Половина датасета
// хуже, чем его отсутствие, поэтому пороги приёмки проверяются здесь.
//
// Зависимостей нет намеренно: всё нужное есть в Node из коробки.

import { spawnSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import { appendFileSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { gunzipSync, gzipSync } from 'node:zlib'

/** Потолок Шикимори на один запрос. */
const BATCH = 50
/** Адреса по порядку: два знает клиент, третий — запасной ход сборщика. */
const MIRRORS = ['shikimori.io', 'shikimori.rip', 'shikimori.one']
/** После скольких отказов подряд обход уходит на следующий адрес. */
const SWITCH_AFTER = 3
/** Осмысленный User-Agent обязателен: без него Шикимори отвечает отказом. */
const UA = 'AniMori/3.0 (+https://github.com/foulnike/animori-data)'
/** Адреса складываются из частей, как зеркала в api/shikimori.ts приложения. */
const GITHUB_API = 'https://api.github.com'
const SHIKI_PATH = '/api/animes?ids='
/** Потолок одного запроса: виснувшее соединение не должно съесть весь прогон. */
const TIMEOUT_MS = 15000
/** Откуда берутся номера. */
const MANAMI = 'manami-project/anime-offline-database'
/** Имя базы в выпуске. Точное: под маску «minified» подходят и списки мёртвых записей. */
const ASSET = 'anime-offline-database-minified.json'
/** Пороги приёмки. Ниже — сборка не состоялась и наружу не выходит. */
const MIN_TITLES = 20000
const MIN_KNOWN = 0.9
/** Через сколько запросов печатается строка о ходе дела. */
const REPORT_EVERY = 50
/** Метки источников в записи манами. Без регэкспов: строк тут сотня тысяч. */
const MAL_MARK = 'myanimelist.net/anime/'
const ANI_MARK = 'anilist.co/anime/'
const CYRILLIC = /[А-Яа-яЁё]/

const PAUSE_MS = Number(process.env.BUILD_PAUSE || 700)
/** Потолок числа номеров: 0 — весь список. Нужен для пробной сборки. */
const LIMIT = Number(process.env.BUILD_LIMIT || 0)

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
 * Разбор отказа сети. У Node в message всегда одно и то же «fetch failed»,
 * а нужное лежит в cause: ENOTFOUND — не разобралось имя, ECONNREFUSED
 * и ETIMEDOUT — адрес закрыт, UND_ERR_CONNECT_TIMEOUT — соединение уронили.
 */
function why(e) {
  const cause = e.cause || {}
  return cause.code || cause.message || e.message || String(e)
}

const sleep = (ms) => new Promise((done) => setTimeout(done, ms))
const round1 = (n) => Math.round(n * 10) / 10
const pct = (n) => `${Math.round(n * 1000) / 10}%`

function bump(counter, key) {
  counter[key] = (counter[key] || 0) + 1
}

async function github(path) {
  const headers = { 'user-agent': UA, accept: 'application/vnd.github+json' }
  if (process.env.GITHUB_TOKEN) headers.authorization = `Bearer ${process.env.GITHUB_TOKEN}`
  const answer = await fetch(GITHUB_API + path, { headers })
  if (!answer.ok) fail(`GitHub ответил ${answer.status} на ${path}`)
  return answer.json()
}

/**
 * Файл базы в выпуске манами. По точному имени: под маску подходят соседи
 * вроде anidb-minified.json — списки мёртвых записей на десятки килобайт.
 * Разбор такого файла проходит, массива data в нём нет.
 */
function pickAsset(assets) {
  for (const ext of ['', '.zst', '.gz']) {
    const hit = assets.find((a) => a.name === ASSET + ext)
    if (hit) return hit
  }
  fail(`в выпуске манами нет ${ASSET}, лежит: ${assets.map((a) => a.name).join(', ')}`)
}

/** Расжатие. zstd гоним через файл: у spawnSync потолок буфера в мегабайт. */
function decompress(name, body) {
  if (name.endsWith('.gz')) return gunzipSync(body)
  if (!name.endsWith('.zst')) return body

  const dir = mkdtempSync(join(tmpdir(), 'manami-'))
  const packed = join(dir, 'base.zst')
  const plain = join(dir, 'base.json')
  writeFileSync(packed, body)
  const run = spawnSync('zstd', ['-d', '-f', '-o', plain, packed], { stdio: 'inherit' })
  if (run.status !== 0) fail(`zstd не расжал базу манами (код ${run.status})`)
  return readFileSync(plain)
}

/**
 * Последний выпуск манами. Именно выпуск, а не raw из ветки: после тега 2025-25
 * базы живут только в выпусках, а старый адрес однажды тихо притащит пустоту.
 */
async function loadManami() {
  const release = await github(`/repos/${MANAMI}/releases/latest`)
  const asset = pickAsset(release.assets || [])
  const sizeMb = round1(asset.size / 1048576)
  console.log(`Манами: выпуск ${release.tag_name}, файл ${asset.name}, ${sizeMb} МБ`)

  const answer = await fetch(asset.browser_download_url, { headers: { 'user-agent': UA } })
  if (!answer.ok) fail(`файл выпуска манами не скачался: HTTP ${answer.status}`)
  const raw = decompress(asset.name, Buffer.from(await answer.arrayBuffer()))

  let base = null
  try {
    base = JSON.parse(raw.toString('utf8'))
  } catch (e) {
    fail(`файл выпуска манами не разобрался как JSON: ${e.message}`)
  }

  const entries = base.data
  if (!Array.isArray(entries)) {
    fail(`в файле ${asset.name} нет массива data, а есть: ${Object.keys(base).join(', ')}`)
  }
  return { tag: release.tag_name, entries }
}

/** Из записи манами нужны только номера: имена всё равно приедут с Шикимори. */
function collectIds(entries) {
  const mal = []
  const pairs = []

  for (const entry of entries) {
    let m = 0
    let a = 0
    for (const source of entry.sources || []) {
      if (!m) {
        const at = source.indexOf(MAL_MARK)
        if (at >= 0) m = Number(source.slice(at + MAL_MARK.length))
      }
      if (!a) {
        const at = source.indexOf(ANI_MARK)
        if (at >= 0) a = Number(source.slice(at + ANI_MARK.length))
      }
    }
    if (!Number.isFinite(m) || m <= 0) continue
    mal.push(m)
    if (Number.isFinite(a) && a > 0) pairs.push([m, a])
  }

  return { mal, pairs }
}

/** Одна пачка к зеркалу. Куки не шлём — клиент тоже не шлёт: с ними бывает 400. */
async function ask(domain, batch) {
  const url = 'https://' + domain + SHIKI_PATH + batch.join(',') + '&limit=' + BATCH
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
 * Полный обход. Пачка не бросается при отказе: она повторяется, а после трёх
 * отказов подряд обход уходит на следующий адрес. Прогон, где первое зеркало
 * молчало на каждой пачке, уже был на пробе — сборка обязана это пережить.
 */
async function crawl(ids) {
  const stat = {
    mirror: MIRRORS[0],
    requests: 0,
    asked: 0,
    answered: 0,
    russian: 0,
    cyrillic: 0,
    codes: {},
    tookMs: 0,
  }
  const rows = []
  const startedAll = Date.now()

  let at = 0
  let mirrorAt = 0
  let misses = 0

  while (at < ids.length) {
    const domain = MIRRORS[mirrorAt]
    const batch = ids.slice(at, at + BATCH)
    let answer = await ask(domain, batch)

    // Один повтор на 429: лимит говорит «подожди», а не «уходи».
    if (answer.status === 429) {
      const wait = Math.max(answer.retryAfter * 1000, 5000)
      console.log(`${domain}: 429, ждём ${wait} мс и повторяем ту же пачку`)
      stat.requests++
      bump(stat.codes, 429)
      await sleep(wait)
      answer = await ask(domain, batch)
    }

    stat.requests++
    bump(stat.codes, answer.status)

    if (answer.status !== 200) {
      misses++
      console.log(`${domain}: пачка ${1 + at / BATCH} — ${answer.status}`)

      if (misses >= SWITCH_AFTER) {
        mirrorAt++
        misses = 0
        if (mirrorAt >= MIRRORS.length) {
          fail(`все адреса Шикимори перестали отвечать на пачке ${1 + at / BATCH}`)
        }
        stat.mirror = MIRRORS[mirrorAt]
        console.log(`Уходим на ${stat.mirror}: ${domain} не отвечает`)
      }

      await sleep(PAUSE_MS)
      continue
    }

    misses = 0
    stat.asked += batch.length

    for (const item of answer.items) {
      const russian = (item.russian || '').trim()
      stat.answered++
      if (russian) stat.russian++
      if (CYRILLIC.test(russian)) stat.cyrillic++
      // Ровно шесть полей: чем меньше в файле, тем быстрее он грузится.
      rows.push({
        id: item.id,
        name: item.name,
        russian,
        kind: item.kind,
        aired_on: item.aired_on,
        score: item.score,
      })
    }

    at += BATCH

    if (stat.requests % REPORT_EVERY === 0) {
      const done = Math.min(at, ids.length)
      const part = pct(done / ids.length)
      console.log(`Обход: ${done} из ${ids.length} (${part}), имён ${rows.length}`)
    }

    await sleep(PAUSE_MS)
  }

  stat.tookMs = Date.now() - startedAll
  return { stat, rows }
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

async function main() {
  console.log(`Сборка: пауза ${PAUSE_MS} мс, потолок номеров ${LIMIT || 'без потолка'}`)

  const manami = await loadManami()
  const { mal, pairs } = collectIds(manami.entries)
  console.log(
    `Манами: записей ${manami.entries.length}, с номером MAL ${mal.length}, ` +
      `с парой MAL+AniList ${pairs.length}`,
  )

  const ids = LIMIT > 0 ? mal.slice(0, LIMIT) : mal
  const { stat, rows } = await crawl(ids)

  const known = stat.asked ? stat.answered / stat.asked : 0
  if (rows.length < MIN_TITLES) {
    fail(`собрано ${rows.length} названий при пороге ${MIN_TITLES}`)
  }
  if (known < MIN_KNOWN) {
    fail(`Шикимори узнал ${pct(known)} номеров при пороге ${pct(MIN_KNOWN)}`)
  }

  const builtAt = new Date().toISOString()
  const head = { v: 1, tag: manami.tag, builtAt }

  const titlesFile = pack(FILE_TITLES, { ...head, count: rows.length, titles: rows })
  const mapFile = pack(FILE_MAP, { ...head, count: pairs.length, pairs })

  const index = {
    version: 1,
    builtAt,
    source: MANAMI,
    sourceTag: manami.tag,
    license: 'ODbL-1.0',
    names: { source: stat.mirror, count: rows.length, cyrillic: stat.cyrillic },
    files: [titlesFile, mapFile],
  }
  writeFileSync(FILE_INDEX, `${JSON.stringify(index, null, 2)}\n`, 'utf8')

  // Уведомление в описании выпуска — это атрибуция по разделу 4.2 ODbL,
  // а не украшение: выпуск обязан называть источник и его недельный тег.
  writeFileSync(
    FILE_NOTES,
    [
      'Русские названия аниме для AniMori.',
      '',
      `Названий: ${rows.length}, из них кириллицей ${stat.cyrillic}.`,
      `Соответствий MAL — AniList: ${pairs.length}.`,
      `Собрано ${builtAt} через ${stat.mirror}.`,
      '',
      `Номера и связки: manami-project/anime-offline-database, выпуск ${manami.tag},`,
      'по лицензии ODbL-1.0, содержимое — DbCL-1.0.',
      'Этот датасет — производная база и выходит на тех же условиях: ODbL-1.0.',
      'Русские названия получены из открытого API Шикимори.',
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

  report([
    `## Сборка датасета: ${manami.tag}`,
    '',
    '| Что | Сколько |',
    '| --- | --- |',
    `| Спрошено номеров | ${stat.asked} |`,
    `| Шикимори знает | ${stat.answered} (${pct(known)}) |`,
    `| Русское имя есть | ${stat.russian} |`,
    `| Из них кириллицей | ${stat.cyrillic} (${pct(stat.cyrillic / stat.answered)}) |`,
    `| Соответствия MAL — AniList | ${pairs.length} |`,
    `| Запросов | ${stat.requests}, ответы: ${codes} |`,
    `| Время обхода | ${round1(stat.tookMs / 60000)} мин через ${stat.mirror} |`,
    `| ${FILE_TITLES} | ${titlesMb} МБ |`,
    `| ${FILE_MAP} | ${mapMb} МБ |`,
    '',
    '**Файлы собраны. Публикация — следующим шагом, если она включена.**',
  ])
}

await main()
