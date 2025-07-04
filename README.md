# Movie JSONL ➜ SQLite Normalised Loader

This repo provides two **stream-friendly ETL utilities** that convert a large TMDB-style *JSONL* movie dump into a fully-normalised, query-friendly **SQLite** database.

Dataset is taken from [Full TMDB Movies Dataset (1M+)](https://www.kaggle.com/datasets/octopusteam/tmdb-movies-dataset) on Kaggle.

* `json_to_sqlite.py`                 – loads **all** movies  
* `json_to_sqlite_filtered.py`        – loads only movies that  
  1. are **not** flagged as adult (`"adult": true`), **and**  
  2. provide a non-empty `poster_path`, **and**  
  3. provide a non-empty `overview`  

  (Because every retained record is guaranteed to be non-adult, the column  
  `adult` is **omitted** from the resulting SQLite schema.)

Both scripts

* flatten nested structures into child tables (`movie_genres`, `movie_videos`, …) instead of storing JSON blobs,
* convert meaningless values (`0`, `""`, `[]`, `{}`) to SQL `NULL`, while Boolean
  fields become the integers **“1”** for `true` / **“0”** for `false`,
* stream the input file in batches of 1000 lines (constant memory),
* apply WAL mode + PRAGMA tweaks for fast bulk inserts,
* print live progress (rows stored / rows skipped / rows-per-second).

---

## 1. Quick start

```bash
# full load
python json_to_sqlite.py           movies.jsonl   # → movies.db

# filtered load (no adult, must have poster & overview)
python json_to_sqlite_filtered.py  movies.jsonl   # → movies.db
```

Optionally pass a custom database filename:

```bash
python json_to_sqlite_filtered.py movies.jsonl  my_movies.db
```

After the run:

```bash
sqlite3 my_movies.db
sqlite> .tables
movies  movie_genres  movie_spoken_languages  movie_origin_countries …
```

---

## 2. Database schema (normalised)

Main table (`movies`) – scalar fields only:

```
id  | title | … | vote_average | poster_path | …
```

`adult` is present **only** when you use `json_to_sqlite.py` (the full loader).  
The filtered loader leaves that column out because every retained record is
guaranteed to be family-friendly.

Child tables (all have a `movie_id` FK-column):

| Table                         | Sample columns (⇢ query examples below)          |
|-------------------------------|--------------------------------------------------|
| `movie_genres`                | `movie_id, genre_id, genre_name`                |
| `movie_spoken_languages`      | `movie_id, iso_639_1, name, english_name`       |
| `movie_origin_countries`      | `movie_id, iso_3166_1`                           |
| `movie_production_companies`  | `movie_id, company_id, name, origin_country`     |
| `movie_production_countries`  | `movie_id, iso_3166_1, name`                     |
| `movie_videos`                | `movie_id, video_id, key, name, site, type …`    |

---

## 3. Example queries

Top 20 comedies by rating:

```sql
SELECT m.title, m.vote_average
FROM   movies m
JOIN   movie_genres g ON g.movie_id = m.id
WHERE  g.genre_name = 'Comedy'
ORDER  BY m.vote_average DESC
LIMIT  20;
```

Production-company leaderboard:

```sql
SELECT pc.name, COUNT(*) AS movies_produced
FROM   movie_production_companies pc
GROUP  BY pc.name
ORDER  BY movies_produced DESC
LIMIT  15;
```

---

## 4. Implementation details

1. **Filtering logic** (only in `json_to_sqlite_filtered.py`)  
   ```python
   def _should_skip(movie):
       return (
           movie.get("adult")                  # skip adult
           or not movie.get("poster_path")     # needs poster
           or not movie.get("overview")        # needs overview
       )
   ```
2. **Normalisation** – `_split_record()` extracts scalar columns and child-row tuples in one pass, keeping memory usage minimal.  
3. **Batch flushing** – every 1000 records all pending rows for **all** tables are written inside a single transaction for referential integrity and speed.  
4. **Value sanitising** – helper `_norm()` converts `0`, `""`, `[]`, `{}` to `None`
   and maps booleans to the strings `"yes"` / `"no"` for convenient querying.

---

## 5. Requirements

* Python 3.8+
* Standard library only (uses `json`, `sqlite3`, `pathlib`, `time`).

The scripts are tested on Fedora 42 but should work identically on any OS with SQLite and Python available.

---

## 6. FAQ

**Q : My input is a single gigantic JSON array, not JSONL.**  
A : Use a tool like `jq` to convert it:  
```bash
jq -c '.[]' movies.json > movies.jsonl
```

**Q : Can I change the batch size?**  
A : Open the script and adjust `self.batch_size = 1000` near the top.

**Q : I need more filters (e.g. minimum votes).**  
A : Edit `_should_skip()` in `json_to_sqlite_filtered.py` and extend the conditions (the file already contains several recommended rules commented-out).

---

Happy data-crunching! 🎬📊
