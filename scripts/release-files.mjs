// Запись выпуска и отчёт о прогоне.
//
// Сборщик считает и проверяет, а этот модуль складывает три файла выпуска
// и называет числа человеку. Ничего не публикует и ничего не решает:
// публикация — отдельный шаг workflow, а пороги приёмки остались в сборщике.
//
// Зависимостей нет намеренно: всё нужное есть в Node из коробки.

import { createHash } from 'node:crypto'
import { appendFileSync, writeFileSync } from 'node:fs'
import { gzipSync } from 'node:zlib'

import { pct, round1 } from './common.mjs'

export const FILE_TITLES = 'titles-anime.json.gz'
export const FILE_MAP = 'map-mal-anilist.json.gz'
export const FILE_INDEX = 'index.json'
export const FILE_NOTES = 'release-notes.md'

/** Пишет сжатый файл и возвращает строку описи: имя, размер, отпечаток. */
export function pack(name, payload) {
  const body = gzipSync(Buffer.from(JSON.stringify(payload), 'utf8'), { level: 9 })
  writeFileSync(name, body)
  return {
    name,
    bytes: body.length,
    sha256: createHash('sha256').update(body).digest('hex'),
  }
}

/** Печатает и в лог, и в итог прогона: за числами не надо лезть в артефакт. */
export function report(lines) {
  const text = lines.join('\n')
  console.log(`\n${text}\n`)
  if (process.env.GITHUB_STEP_SUMMARY) {
    appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${text}\n`, 'utf8')
  }
}

/**
 * Строка отчёта о волне: пропущена, свёрнута или отработала целиком.
 * Про disabled знает только волна карты, у волны имён поля нет — и не надо.
 */
export function waveLine(stat, done) {
  if (stat.skipped) return 'выключена настройкой'
  if (stat.disabled) return 'сторонний доступ к AniList закрыт, волна свёрнута сразу'
  const tail = stat.gaveUp ? ', волна свёрнута досрочно' : ''
  return `${done} за ${round1(stat.tookMs / 60000)} мин${tail}`
}

/**
 * Разница против прошлого выпуска: новых записей, правок и пропаж.
 *
 * Частичный обход считает новых и обновлённых сам — он идёт от семени,
 * и другого способа у него нет. Полный обход семени не видит вовсе и
 * возвращает «новых столько же, сколько записей»: это не факт о каталоге,
 * а способ сказать «перечислено всё заново». В отчёте такое число врёт —
 * прогон 14 сентября 2026 объявил 30 545 новых записей при шестидесяти
 * четырёх настоящих. Поэтому при полном обходе разница считается здесь,
 * по тому самому семени, которое всё равно качалось ради выбора вида обхода.
 *
 * Пропажи видны только отсюда: узнать о них, не перечислив каталог целиком,
 * нельзя, и частичный обход их не замечает принципиально.
 */
export function diffSeed(rows, seedRows) {
  const was = new Map()
  for (const row of seedRows) was.set(row.id, row)

  let added = 0
  let changed = 0

  for (const row of rows) {
    const known = was.get(row.id)
    if (known === undefined) {
      added++
      continue
    }
    if (known.russian !== row.russian || known.aired_on !== row.aired_on) changed++
    // Остаток карты — это записи семени, которых в каталоге больше нет.
    was.delete(row.id)
  }

  return { added, changed, gone: was.size }
}

/**
 * Складывает выпуск: два сжатых файла, опись и описание, и печатает
 * таблицу итога.
 *
 * Версия описи остаётся первой: поля только добавляются, и старый клиент
 * читает её как прежде. Поле count как было числом записей, так и осталось:
 * менять смысл имеющегося поля значило бы соврать всем, кто его читает.
 *
 * names.known остаётся ради совместимости, но смысла в нём больше нет: при
 * перечислении мы получаем ровно то, что каталог отдал, и доля узнанных
 * номеров тождественно равна единице. Живую проверку делает сравнение
 * с прошлым выпуском в сборщике, а не этот порог.
 *
 * names.mode и names.fullAt — новые. Первое говорит, каким был обход этого
 * прогона, второе называет последний полный и передаётся из выпуска
 * в выпуск: именно по нему следующий прогон решает, пора ли идти полным,
 * и другого места для этой памяти нет — сборщик в репозиторий не пишет.
 *
 * license — CC0-1.0, полный отказ от прав. Путь был такой: ODbL-1.0 стояла
 * не по выбору, а приезжала вместе с производностью от манами; производности
 * больше нет, а ни Шикимори, ни anime365, ни AniList условий на выгрузку
 * через открытый API не налагают. Коротко стояла MIT — и тоже не к месту:
 * это лицензия для кода. CC0 говорит прямо про базы данных и права
 * на извлечение данных — ровно про то, чем эти файлы и являются.
 *
 * mapRebuild — прогон, в котором пары спрошены у AniList заново целиком.
 * Семя в таком прогоне не отменяется, а служит опорой: пара уходит из карты
 * только там, где AniList ответил успешно и пары в ответе не оказалось,
 * а номер, до которого волна не дошла, остаётся как в семени. Поэтому
 * в отчёте названы обе стороны правки — и добор, и уборка: пересборка
 * умеет уменьшать карту, и молчать об этом нельзя.
 */
export function writeRelease(ctx) {
  const {
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
    mapRebuild,
    baseline,
    seed,
    seedTitles,
  } = ctx

  // Разница против прошлого выпуска. Частичный обход принёс её с собой,
  // полному считаем её здесь: его собственные числа значат другое.
  const diff =
    mode === 'full' && seedTitles !== null
      ? diffSeed(rows, seedTitles.rows)
      : { added, changed, gone: null }

  const head = { v: 1, tag: sourceTag, builtAt }
  const titlesFile = pack(FILE_TITLES, { ...head, count: rows.length, titles: rows })
  // У карты своя голова: когда она унаследована, тег и дата в файле обязаны
  // называть выпуск, из которого пары пришли, а не сегодняшний прогон. Иначе
  // файл врёт сам про себя, и застой не виден даже в нём.
  const mapHead = { v: 1, tag: mapFrom, builtAt: mapBuiltAt }
  const mapFile = pack(FILE_MAP, { ...mapHead, count: pairs.length, pairs })

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
      mode,
      fullAt,
      added: diff.added,
      changed: diff.changed,
      // Пропажи считает только полный обход; у частичного здесь null,
      // и это честнее нуля: ноль читался бы как «никто не пропал».
      gone: diff.gone,
    },
    // Карта отдельным разделом. Клиенту он не нужен — тот читает files, —
    // а сторожу и человеку нужен: без возраста карты застой в ней неотличим
    // от исправной сборки: число пар остаётся то же самое.
    //
    // rebuilt называет прогоны, в которых пары спрошены заново целиком:
    // иначе по выпуску не отличить освежённую карту от наращенной поверх
    // старого ядра. Рядом с added стоят changed и dropped: пересборка умеет
    // и переносить пару на другой номер, и убирать её вовсе, а по одному
    // числу пар этого не видно.
    map: {
      count: pairs.length,
      from: mapFrom,
      builtAt: mapBuiltAt,
      inherited: mapAged,
      rebuilt: mapRebuild === true,
      added: map.stat.added,
      changed: map.stat.changed,
      dropped: map.stat.dropped,
      kept: map.stat.kept,
      seeded: map.stat.seeded,
      wave: mapWave,
    },
    // Свежесть содержимого, а не файла. Сторож сравнивает maxId с текущей
    // головой Шикимори и ловит застой, при котором выпуски выходят исправно,
    // а содержимое в них не меняется.
    freshness: {
      maxId: fresh.maxId,
      freshDays: fresh.freshDays,
      airedRecent: fresh.airedRecent,
      released: fresh.released,
      ongoing: fresh.ongoing,
      anons: fresh.anons,
    },
    files: [titlesFile, mapFile],
  }
  writeFileSync(FILE_INDEX, `${JSON.stringify(index, null, 2)}\n`, 'utf8')

  // Источник и лицензия называются в описании выпуска намеренно: тот, кто
  // скачал файлы напрямую, за условиями в репозиторий не пойдёт.
  writeFileSync(
    FILE_NOTES,
    [
      'Русские названия аниме для AniMori.',
      '',
      `Записей: ${rows.length}, с русским названием: ${total.russian}, ` +
        `из них кириллицей: ${total.cyrillic}.`,
      mapAged
        ? `Соответствий MAL — AniList: ${pairs.length}. Карта унаследована ` +
          `от выпуска ${mapFrom} (${mapBuiltAt}): AniList в этот прогон не ответил.`
        : mapRebuild
          ? `Соответствий MAL — AniList: ${pairs.length}. Карта переспрошена ` +
            `у AniList целиком: добрано ${map.stat.added}, убрано ${map.stat.dropped} ` +
            'пар, отсутствие которых AniList подтвердил ответом.'
          : `Соответствий MAL — AniList: ${pairs.length}, добрано в этот прогон: ` +
            `${map.stat.added}.`,
      `Собрано ${builtAt} через ${stat.mirror}, обход ` +
        `${mode === 'full' ? 'полный' : 'частичный'}.`,
      `Голова каталога — номер ${fresh.maxId}, за последние ${fresh.freshDays} дней ` +
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
  const baseLine = baseline === null ? 'нет базы сравнения' : `${baseline.count} записей`
  // При пересборке семя не отменяется, а держит карту: пара уходит только
  // по успешному ответу AniList без неё. Строка говорит об этом прямо —
  // прежде она говорила обратное, и это было неправдой.
  const seedTail = mapRebuild ? ', опора пересборки' : ''
  const seedLine =
    seed === null ? 'нет' : `${seed.pairs.length} пар от ${seed.tag || 'без тега'}${seedTail}`
  const seedTitlesLine =
    seedTitles === null
      ? 'нет, обход полный'
      : `${seedTitles.rows.length} записей от ${seedTitles.tag || 'без тега'}`
  const mapAgeLine = mapAged
    ? `унаследована от ${mapFrom}, ${mapBuiltAt}`
    : mapRebuild
      ? 'этот прогон, собрана заново целиком'
      : 'этот прогон'
  const diffLine = seedTitles === null ? 'нет базы сравнения' : String(diff.added)
  const changedLine = seedTitles === null ? 'нет базы сравнения' : String(diff.changed)
  const mapEdits =
    `добрано ${map.stat.added}, переехало ${map.stat.changed}, убрано ${map.stat.dropped}` +
    (map.stat.kept > 0 ? `, оставлено от семени ${map.stat.kept}` : '')

  report([
    `## Сборка датасета: ${sourceTag}`,
    '',
    '| Что | Сколько |',
    '| --- | --- |',
    `| Вид обхода | ${mode === 'full' ? 'полный' : 'частичный'} |`,
    `| Семя имён | ${seedTitlesLine} |`,
    `| Страниц каталога | ${stat.pages} |`,
    `| Записей всего | ${rows.length} |`,
    `| Новых против прошлого выпуска | ${diffLine} |`,
    `| Обновлённых записей | ${changedLine} |`,
    ...(diff.gone === null ? [] : [`| Пропало из каталога | ${diff.gone} |`]),
    `| Голова каталога | ${fresh.maxId} |`,
    `| Прошлый выпуск | ${baseLine} |`,
    `| Русское имя от Шикимори | ${fromShiki.russian} |`,
    `| Добрано с anime365 | ${extra.stat.added} из ${extra.stat.empty} пустых |`,
    `| Русское имя всего | ${total.russian} (${pct(total.russian / rows.length)}) |`,
    `| Из них кириллицей | ${total.cyrillic} |`,
    `| Семя карты | ${seedLine} |`,
    `| Правки карты | ${mapEdits} |`,
    `| Пары всего | ${pairs.length} (${pct(pairs.length / rows.length)} от записей) |`,
    `| Возраст карты | ${mapAgeLine} |`,
    `| Вышло за ${fresh.freshDays} дн. | ${fresh.airedRecent} |`,
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
