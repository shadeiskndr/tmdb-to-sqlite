#!/usr/bin/env python3
"""
JSON-to-SQLite ETL
Version: “normalised”
• No JSON blobs stored in the DB
• 0 / '' / [] / {} are converted to SQL NULL
• Nested arrays are written to separate child tables
"""

import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────
def _norm(v: Any) -> Any:
    """
    Prepare a raw JSON value for SQLite.

    • Booleans        → "yes" / "no"
    • 0, '', [], {}   → NULL          (None in Python)
    """
    # handle booleans first (bool is a subclass of int!)
    if isinstance(v, bool):
        return "yes" if v else "no"

    if v in ("", [], {}, None):
        return None
    if isinstance(v, (int, float)) and v == 0:
        return None
    return v


# ──────────────────────────────────────────────────────────────────────
# Main class
# ──────────────────────────────────────────────────────────────────────
class JSONToSQLiteTransfer:
    def __init__(self, json_file: str, db_file: str = "movies.db", table_name: str = "movies"):
        self.json_file      = Path(json_file)
        self.db_file        = Path(db_file)
        self.table_name     = table_name
        self.batch_size     = 1_000

        # Scalar columns that stay in the main “movies” table
        self.scalar_schema = {
            'id': 'INTEGER PRIMARY KEY',
            'adult': 'BOOLEAN',
            'title': 'TEXT',
            'original_title': 'TEXT',
            'video': 'BOOLEAN',
            'budget': 'INTEGER',
            'revenue': 'INTEGER',
            'runtime': 'INTEGER',
            'status': 'TEXT',
            'imdb_id': 'TEXT',
            'tagline': 'TEXT',
            'homepage': 'TEXT',
            'overview': 'TEXT',
            'popularity': 'REAL',
            'vote_count': 'INTEGER',
            'vote_average': 'REAL',
            'release_date': 'TEXT',
            'original_language': 'TEXT',
            'poster_path': 'TEXT',
            'backdrop_path': 'TEXT',

            # collection
            'collection_id': 'INTEGER',
            'collection_name': 'TEXT',
            'collection_poster_path': 'TEXT',
            'collection_backdrop_path': 'TEXT',

            # external ids
            'external_imdb_id': 'TEXT',
            'external_twitter_id': 'TEXT',
            'external_facebook_id': 'TEXT',
            'external_wikidata_id': 'TEXT',
            'external_instagram_id': 'TEXT',
        }

        # Child table DDL ‑ one entry: (table_name, create_sql, insert_sql, columns_tuple_len)
        self.child_tables = {
            'movie_genres': (
                """CREATE TABLE IF NOT EXISTS movie_genres (
                       movie_id INTEGER,
                       genre_id INTEGER,
                       genre_name TEXT
                   )""",
                "INSERT INTO movie_genres (movie_id, genre_id, genre_name) VALUES (?,?,?)",
                3
            ),
            'movie_spoken_languages': (
                """CREATE TABLE IF NOT EXISTS movie_spoken_languages (
                       movie_id INTEGER,
                       iso_639_1 TEXT,
                       name TEXT,
                       english_name TEXT
                   )""",
                "INSERT INTO movie_spoken_languages (movie_id, iso_639_1, name, english_name) VALUES (?,?,?,?)",
                4
            ),
            'movie_origin_countries': (
                """CREATE TABLE IF NOT EXISTS movie_origin_countries (
                       movie_id INTEGER,
                       iso_3166_1 TEXT
                   )""",
                "INSERT INTO movie_origin_countries (movie_id, iso_3166_1) VALUES (?,?)",
                2
            ),
            'movie_production_companies': (
                """CREATE TABLE IF NOT EXISTS movie_production_companies (
                       movie_id INTEGER,
                       company_id INTEGER,
                       name TEXT,
                       origin_country TEXT,
                       logo_path TEXT
                   )""",
                "INSERT INTO movie_production_companies (movie_id, company_id, name, origin_country, logo_path) VALUES (?,?,?,?,?)",
                5
            ),
            'movie_production_countries': (
                """CREATE TABLE IF NOT EXISTS movie_production_countries (
                       movie_id INTEGER,
                       iso_3166_1 TEXT,
                       name TEXT
                   )""",
                "INSERT INTO movie_production_countries (movie_id, iso_3166_1, name) VALUES (?,?,?)",
                3
            ),
            'movie_videos': (
                """CREATE TABLE IF NOT EXISTS movie_videos (
                       movie_id INTEGER,
                       video_id TEXT,
                       key TEXT,
                       name TEXT,
                       site TEXT,
                       size INTEGER,
                       type TEXT,
                       official BOOLEAN,
                       published_at TEXT
                   )""",
                "INSERT INTO movie_videos (movie_id, video_id, key, name, site, size, type, official, published_at) VALUES (?,?,?,?,?,?,?,?,?)",
                9
            ),
        }

    # ──────────────────────────────────────────────────────────────
    # DB helpers
    # ──────────────────────────────────────────────────────────────
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_file)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _create_tables(self, conn: sqlite3.Connection) -> None:
        # Drop existing tables
        conn.execute(f"DROP TABLE IF EXISTS {self.table_name}")
        for tbl in self.child_tables:
            conn.execute(f"DROP TABLE IF EXISTS {tbl}")

        # Main table
        cols = ", ".join(f"{c} {t}" for c, t in self.scalar_schema.items())
        conn.execute(f"CREATE TABLE {self.table_name} ({cols})")

        # Child tables
        for create_sql, _, _ in self.child_tables.values():
            conn.execute(create_sql)

        conn.commit()

    # ──────────────────────────────────────────────────────────────
    # JSON  → rows
    # ──────────────────────────────────────────────────────────────
    def _split_record(self, movie: Dict[str, Any]) -> Tuple[Tuple, Dict[str, List[Tuple]]]:
        """
        Returns
        -------
        main_row : Tuple        # matches self.scalar_schema order
        children  : Dict[str,List[Tuple]]
        """
        scalar_cols = []
        for col in self.scalar_schema:
            # special handling for nested fields
            if col.startswith("collection_"):
                collection = movie.get("belongs_to_collection") or {}
                key = col.replace("collection_", "")
                scalar_cols.append(_norm(collection.get(key)))
            elif col.startswith("external_"):
                external = movie.get("external_ids") or {}
                key = col.replace("external_", "")
                scalar_cols.append(_norm(external.get(key)))
            else:
                scalar_cols.append(_norm(movie.get(col)))
        main_row = tuple(scalar_cols)

        mid = movie["id"]
        children: Dict[str, List[Tuple]] = {k: [] for k in self.child_tables}

        # genres
        for g in movie.get("genres", []):
            children['movie_genres'].append(
                (mid, _norm(g.get("id")), _norm(g.get("name")))
            )

        # spoken_languages
        for lang in movie.get("spoken_languages", []):
            children['movie_spoken_languages'].append(
                (mid,
                 _norm(lang.get("iso_639_1")),
                 _norm(lang.get("name")),
                 _norm(lang.get("english_name")))
            )

        # origin_country (simple ISO list)
        for iso in movie.get("origin_country", []):
            children['movie_origin_countries'].append(
                (mid, _norm(iso))
            )

        # production_companies
        for pc in movie.get("production_companies", []):
            children['movie_production_companies'].append(
                (mid,
                 _norm(pc.get("id")),
                 _norm(pc.get("name")),
                 _norm(pc.get("origin_country")),
                 _norm(pc.get("logo_path")))
            )

        # production_countries (objects)
        for c in movie.get("production_countries", []):
            children['movie_production_countries'].append(
                (mid,
                 _norm(c.get("iso_3166_1")),
                 _norm(c.get("name")))
            )

        # videos
        for v in movie.get("videos", {}).get("results", []):
            children['movie_videos'].append(
                (mid,
                 _norm(v.get("id")),
                 _norm(v.get("key")),
                 _norm(v.get("name")),
                 _norm(v.get("site")),
                 _norm(v.get("size")),
                 _norm(v.get("type")),
                 _norm(v.get("official")),
                 _norm(v.get("published_at")))
            )

        return main_row, children

    # ──────────────────────────────────────────────────────────────
    # Transfer
    # ──────────────────────────────────────────────────────────────
    def transfer(self) -> None:
        if not self.json_file.exists():
            print(f"File {self.json_file} not found.")
            sys.exit(1)

        conn = self._connect()
        self._create_tables(conn)

        # Prepare insert statements
        main_cols = ", ".join(self.scalar_schema.keys())
        placeholders = ", ".join("?" * len(self.scalar_schema))
        MAIN_SQL = f"INSERT OR REPLACE INTO {self.table_name} ({main_cols}) VALUES ({placeholders})"

        child_insert_sql = {t: insert_sql for t, (_, insert_sql, _) in self.child_tables.items()}

        # Batch containers
        batch_main: List[Tuple] = []
        batch_children: Dict[str, List[Tuple]] = {tbl: [] for tbl in self.child_tables}

        start = time.time()
        total = 0
        with open(self.json_file, encoding="utf-8") as fh:
            for ln, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    movie = json.loads(line)
                except json.JSONDecodeError:
                    print(f"[WARN] Skipped bad JSON at line {ln}")
                    continue

                main_row, children = self._split_record(movie)
                batch_main.append(main_row)
                for tbl, rows in children.items():
                    batch_children[tbl].extend(rows)

                if len(batch_main) >= self.batch_size:
                    self._flush(conn, MAIN_SQL, batch_main, child_insert_sql, batch_children)
                    total += self.batch_size
                    elapsed = time.time() - start
                    print(f"\r{total:,} rows │ {elapsed:,.1f}s │ {total/elapsed:,.0f} r/s", end='')
        # flush remainder
        self._flush(conn, MAIN_SQL, batch_main, child_insert_sql, batch_children)
        total += len(batch_main)
        elapsed = time.time() - start
        print(f"\rFinished: {total:,} rows in {elapsed:.1f}s  ({total/elapsed:,.0f} r/s)")

        conn.close()

    # ──────────────────────────────────────────────────────────────
    # Flush helpers
    # ──────────────────────────────────────────────────────────────
    @staticmethod
    def _flush(conn: sqlite3.Connection,
               main_sql: str,
               batch_main: List[Tuple],
               child_sql: Dict[str, str],
               batch_children: Dict[str, List[Tuple]]) -> None:
        if not batch_main:
            return
        with conn:                    # single transaction
            conn.executemany(main_sql, batch_main)
            batch_main.clear()
            for tbl, rows in batch_children.items():
                if rows:
                    conn.executemany(child_sql[tbl], rows)
                    rows.clear()


# ──────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────
def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python json_to_sqlite.py <jsonl_file> [movies.db]")
        sys.exit(1)
    json_file = sys.argv[1]
    db_file   = sys.argv[2] if len(sys.argv) > 2 else "movies.db"

    JSONToSQLiteTransfer(json_file, db_file).transfer()


if __name__ == "__main__":
    main()
