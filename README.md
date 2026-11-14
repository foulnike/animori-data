# animori-data

Репозиторий данных для [AniMori](https://github.com/foulnike/Animori).
GitHub Actions собирает и публикует наборы данных, которые приложение загружает без авторизации.

## Назначение

Репозиторий предоставляет:

- русские названия и основные сведения об аниме;
- соответствия идентификаторов MyAnimeList и AniList;
- `index.json` с метаданными выпуска, контрольными суммами и ссылками на файлы.

Файлы последнего выпуска доступны по постоянным адресам:

```text
https://github.com/foulnike/animori-data/releases/latest/download/index.json
https://github.com/foulnike/animori-data/releases/latest/download/titles-anime.json.gz
https://github.com/foulnike/animori-data/releases/latest/download/map-mal-anilist.json.gz
```

Сборка и публикация выполняются workflow `build-names.yml`. Выпуски хранятся в
[GitHub Releases](https://github.com/foulnike/animori-data/releases).

## Лицензия

[CC0-1.0](LICENSE).

## Связь с разработчиком

Почта: foulnike@protonmail.com — вопросы и предложения.
Ошибки можно сообщить через [Issues](https://github.com/foulnike/animori-data/issues).
