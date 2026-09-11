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
 * mapRebuild — просьба пересобрать карту без семени. В отчёте она названа
 * намеренно: семя в таком прогоне скачано и служит порогом приёмки,
 * но в волну не шло, и строка «Семя карты: 20929 пар» без оговорки врала бы.
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
      added,
      changed,
    },
    // Карта отдельным разделом. Клиенту он не нужен — тот читает files, —
    // а сторожу и человеку нужен: без возраста карты застой в ней неотличим
    // от исправной сборки: число пар остаётся то же самое.
    //
    // rebuilt называет прогоны, в которых пары спрошены заново целиком:
    // иначе по выпуску не отличить освежённую карту от наращенной поверх
    // старого ядра.
    map: {
      count: pairs.length,
      from: mapFrom,
      builtAt: mapBuiltAt,
      inherited: mapAged,
      rebuilt: mapRebuild === true,
      added: map.stat.added,
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
          ? `Соответствий MAL — AniList: ${pairs.length}. Карта собрана заново ` +
            'целиком: семя прошлого выпуска в волну не шло.'
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
  // При пересборке семя скачано и служит порогом приёмки, но в волну не
  // шло: строка без оговорки читалась бы как обычный прогон.
  const seedTail = mapRebuild ? ', в волну не шло: пересборка' : ''
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

  report([
    `## Сборка датасета: ${sourceTag}`,
    '',
    '| Что | Сколько |',
    '| --- | --- |',
    `| Вид обхода | ${mode === 'full' ? 'полный' : 'частичный'} |`,
    `| Семя имён | ${seedTitlesLine} |`,
    `| Страниц каталога | ${stat.pages} |`,
    `| Записей всего | ${rows.length} |`,
    `| Новых за прогон | ${added} |`,
    `| Обновлённых записей | ${changed} |`,
    `| Голова каталога | ${fresh.maxId} |`,
    `| Прошлый выпуск | ${baseLine} |`,
    `| Русское имя от Шикимори | ${fromShiki.russian} |`,
    `| Добрано с anime365 | ${extra.stat.added} из ${extra.stat.empty} пустых |`,
    `| Русское имя всего | ${total.russian} (${pct(total.russian / rows.length)}) |`,
    `| Из них кириллицей | ${total.cyrillic} |`,
    `| Семя карты | ${seedLine} |`,
    `| Пары добраны с AniList | ${map.stat.added} |`,
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
