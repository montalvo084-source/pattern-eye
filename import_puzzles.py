#!/usr/bin/env python3
"""
import_puzzles.py — Import Lichess puzzle database into SQLite.

Usage:
    python3 import_puzzles.py lichess_db_puzzle.csv.zst
    python3 import_puzzles.py --download
    python3 import_puzzles.py lichess_db_puzzle.csv.zst --limit 10000
    python3 import_puzzles.py lichess_db_puzzle.csv.zst --db /path/to/puzzles.db

Download the puzzle database from: https://database.lichess.org/#puzzles
"""

import sys
import csv
import io
import sqlite3
import argparse
import os

LICHESS_PUZZLE_URL = 'https://database.lichess.org/lichess_db_puzzle.csv.zst'


def download_puzzle_file(dest: str = 'lichess_db_puzzle.csv.zst') -> str:
    import urllib.request
    print(f'Downloading puzzle database from Lichess (~280 MB)...')
    print(f'Saving to: {dest}')

    def _progress(count, block_size, total_size):
        if total_size > 0:
            pct = min(count * block_size * 100 // total_size, 100)
            print(f'\r  {pct}%', end='', flush=True)

    urllib.request.urlretrieve(LICHESS_PUZZLE_URL, dest, _progress)
    print('\nDownload complete.')
    return dest


def open_puzzle_file(path: str):
    if path.endswith('.zst'):
        try:
            import zstandard as zstd
        except ImportError:
            print('Error: zstandard package required for .zst files. Run: pip3 install zstandard')
            sys.exit(1)
        fh = open(path, 'rb')
        dctx = zstd.ZstdDecompressor()
        reader = dctx.stream_reader(fh)
        text_stream = io.TextIOWrapper(reader, encoding='utf-8')
        return csv.DictReader(text_stream), fh
    else:
        fh = open(path, 'r', encoding='utf-8')
        return csv.DictReader(fh), fh


def import_puzzles(csv_path: str, db_path: str = 'puzzles.db', limit: int = 50_000) -> None:
    print(f'Importing up to {limit:,} puzzles from {csv_path} into {db_path}...')

    conn = sqlite3.connect(db_path)
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS puzzles (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            lichess_id TEXT UNIQUE NOT NULL,
            fen        TEXT NOT NULL,
            moves      TEXT NOT NULL,
            rating     INTEGER NOT NULL,
            themes     TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_puzzles_rating ON puzzles(rating);
    ''')

    reader, fh = open_puzzle_file(csv_path)
    batch = []
    imported = 0
    skipped = 0

    try:
        for i, row in enumerate(reader):
            if imported >= limit:
                break

            puzzle_id = row.get('PuzzleId', '').strip()
            fen = row.get('FEN', '').strip()
            moves = row.get('Moves', '').strip()
            rating_str = row.get('Rating', '').strip()
            themes = row.get('Themes', '').strip()

            if not all([puzzle_id, fen, moves, rating_str]):
                skipped += 1
                continue

            if len(moves.split()) < 2:
                skipped += 1
                continue

            try:
                rating = int(rating_str)
            except ValueError:
                skipped += 1
                continue

            batch.append((puzzle_id, fen, moves, rating, themes))

            if len(batch) >= 1000:
                conn.executemany(
                    'INSERT OR IGNORE INTO puzzles(lichess_id, fen, moves, rating, themes) VALUES(?,?,?,?,?)',
                    batch
                )
                conn.commit()
                imported += len(batch)
                batch.clear()
                if imported % 5000 == 0:
                    print(f'  {imported:,} imported, {skipped:,} skipped...')

        if batch:
            conn.executemany(
                'INSERT OR IGNORE INTO puzzles(lichess_id, fen, moves, rating, themes) VALUES(?,?,?,?,?)',
                batch
            )
            conn.commit()
            imported += len(batch)

    finally:
        fh.close()
        conn.close()

    print(f'\nDone! {imported:,} puzzles imported, {skipped:,} rows skipped.')
    print(f'Database: {db_path}')
    print('\nRun the app: python3 app.py')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import Lichess puzzles into SQLite')
    parser.add_argument('csv_file', nargs='?', help='Path to lichess_db_puzzle.csv or .csv.zst')
    parser.add_argument('--download', action='store_true', help='Download puzzle file from Lichess automatically')
    parser.add_argument('--limit', type=int, default=50_000, help='Max rows to import (default: 50000)')
    parser.add_argument('--db', default=os.environ.get('DB_PATH', 'puzzles.db'), help='SQLite database path')
    args = parser.parse_args()

    if args.download:
        csv_path = download_puzzle_file()
    elif args.csv_file:
        csv_path = args.csv_file
    else:
        parser.error('Provide a csv_file path or use --download to fetch from Lichess.')

    import_puzzles(csv_path, db_path=args.db, limit=args.limit)
