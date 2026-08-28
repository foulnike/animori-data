// Мелочи, нужные и сборщику, и обеим волнам обогащения.
//
// Отдельным файлом, а не копиями в каждом: разбор отказа сети — не то место,
// где хочется чинить одну и ту же ошибку трижды.

/** Пауза между запросами. */
export const sleep = (ms) => new Promise((done) => setTimeout(done, ms))

/** Одна цифра после запятой: в отчёте больше не нужно. */
export const round1 = (n) => Math.round(n * 10) / 10

/** Доля строкой с процентом. */
export const pct = (n) => `${Math.round(n * 1000) / 10}%`

/** Счётчик по ключу: коды ответов удобнее копить так. */
export function bump(counter, key) {
  counter[key] = (counter[key] || 0) + 1
}

/**
 * Разбор отказа сети. У Node в message всегда одно и то же «fetch failed»,
 * а нужное лежит в cause: ENOTFOUND — не разобралось имя, ECONNREFUSED
 * и ETIMEDOUT — адрес закрыт, UND_ERR_CONNECT_TIMEOUT — соединение уронили.
 */
export function why(e) {
  const cause = e.cause || {}
  return cause.code || cause.message || e.message || String(e)
}
