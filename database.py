import os
import sqlite3
import json
import datetime
from collections import Counter
from flask import g

DB_PATH = os.environ.get('DB_PATH', 'puzzles.db')


def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


def close_db(e=None):
    db = g.pop('db', None)
    if db is not None:
        db.close()


def init_db(app):
    app.teardown_appcontext(close_db)
    with app.app_context():
        os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        db.executescript('''
            CREATE TABLE IF NOT EXISTS puzzles (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                lichess_id TEXT UNIQUE NOT NULL,
                fen        TEXT NOT NULL,
                moves      TEXT NOT NULL,
                rating     INTEGER NOT NULL,
                themes     TEXT NOT NULL DEFAULT ''
            );

            CREATE INDEX IF NOT EXISTS idx_puzzles_rating ON puzzles(rating);

            CREATE TABLE IF NOT EXISTS user_progress (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                puzzle_id          INTEGER NOT NULL REFERENCES puzzles(id),
                result             TEXT NOT NULL CHECK(result IN ('correct','incorrect','skipped')),
                time_taken_seconds INTEGER,
                attempted_at       TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_progress_puzzle ON user_progress(puzzle_id);
            CREATE INDEX IF NOT EXISTS idx_progress_result ON user_progress(result);

            CREATE TABLE IF NOT EXISTS user_stats (
                id                INTEGER PRIMARY KEY CHECK(id = 1),
                current_rating    INTEGER NOT NULL DEFAULT 800,
                session_streak    INTEGER NOT NULL DEFAULT 0,
                total_solved      INTEGER NOT NULL DEFAULT 0,
                weak_themes       TEXT NOT NULL DEFAULT '[]'
            );

            CREATE TABLE IF NOT EXISTS sr_queue (
                puzzle_id    INTEGER PRIMARY KEY REFERENCES puzzles(id),
                sessions_due INTEGER NOT NULL DEFAULT 0,
                fail_count   INTEGER NOT NULL DEFAULT 1,
                added_at     TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS rating_history (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                rating      INTEGER NOT NULL,
                recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
        ''')
        db.execute('INSERT OR IGNORE INTO user_stats(id) VALUES(1)')
        db.commit()

        # Safe migrations for existing databases
        for migration in [
            "ALTER TABLE user_stats ADD COLUMN last_trained_date TEXT NOT NULL DEFAULT ''",
        ]:
            try:
                db.execute(migration)
                db.commit()
            except Exception:
                pass

        db.close()


def get_puzzle(puzzle_id: int):
    db = get_db()
    return db.execute('SELECT * FROM puzzles WHERE id = ?', (puzzle_id,)).fetchone()


def get_puzzles_for_session(rating: int, weak_themes: list) -> list:
    db = get_db()

    def _fetch(low, high, limit=10):
        if weak_themes:
            like_clauses = ' OR '.join(['themes LIKE ?' for _ in weak_themes])
            params = [low, high] + [f'%{t}%' for t in weak_themes] + [limit]
            rows = db.execute(
                f'''SELECT id FROM puzzles
                    WHERE rating BETWEEN ? AND ?
                    ORDER BY CASE WHEN ({like_clauses}) THEN 0 ELSE 1 END, RANDOM()
                    LIMIT ?''',
                params
            ).fetchall()
        else:
            rows = db.execute(
                'SELECT id FROM puzzles WHERE rating BETWEEN ? AND ? ORDER BY RANDOM() LIMIT ?',
                (low, high, limit)
            ).fetchall()
        return [r['id'] for r in rows]

    ids = _fetch(rating, rating + 100)
    if len(ids) < 10:
        ids = _fetch(max(400, rating - 200), rating + 200)
    if len(ids) < 10:
        rows = db.execute('SELECT id FROM puzzles ORDER BY RANDOM() LIMIT 10').fetchall()
        ids = [r['id'] for r in rows]

    return ids[:10]


def get_user_stats():
    db = get_db()
    return db.execute('SELECT * FROM user_stats WHERE id = 1').fetchone()


def update_user_stats(rating_delta: int, solved_delta: int):
    db = get_db()
    new_rating_expr = 'MAX(400, current_rating + ?)' if rating_delta < 0 else 'current_rating + ?'
    db.execute(
        f'UPDATE user_stats SET current_rating = {new_rating_expr}, total_solved = total_solved + ? WHERE id = 1',
        (rating_delta, solved_delta)
    )
    db.commit()


def update_session_streak(correct_count: int):
    db = get_db()
    row = db.execute('SELECT session_streak, last_trained_date FROM user_stats WHERE id = 1').fetchone()
    today = datetime.date.today().isoformat()
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    last_date = row['last_trained_date'] if row['last_trained_date'] else ''

    if correct_count == 0:
        db.execute(
            'UPDATE user_stats SET session_streak = 0, last_trained_date = ? WHERE id = 1',
            (today,)
        )
    elif last_date == today:
        pass  # already counted today
    elif last_date == yesterday:
        db.execute(
            'UPDATE user_stats SET session_streak = session_streak + 1, last_trained_date = ? WHERE id = 1',
            (today,)
        )
    else:
        db.execute(
            'UPDATE user_stats SET session_streak = 1, last_trained_date = ? WHERE id = 1',
            (today,)
        )
    db.commit()


def record_attempt(puzzle_id: int, result: str, time_taken: int):
    db = get_db()
    db.execute(
        'INSERT INTO user_progress(puzzle_id, result, time_taken_seconds) VALUES(?,?,?)',
        (puzzle_id, result, time_taken)
    )
    db.commit()


def recalculate_weak_themes() -> list:
    db = get_db()
    rows = db.execute(
        '''SELECT p.themes FROM user_progress up
           JOIN puzzles p ON p.id = up.puzzle_id
           WHERE up.result IN ('incorrect', 'skipped')
           AND p.themes != ''
        '''
    ).fetchall()

    counts = Counter()
    for row in rows:
        for theme in row['themes'].split():
            if theme not in ('middlegame', 'endgame', 'opening', 'crushing', 'advantage', 'master', 'masterVsMaster'):
                counts[theme] += 1

    return [theme for theme, _ in counts.most_common(3)]


def update_weak_themes(themes: list):
    db = get_db()
    db.execute('UPDATE user_stats SET weak_themes = ? WHERE id = 1', (json.dumps(themes),))
    db.commit()


def get_lifetime_stats() -> dict:
    db = get_db()
    total = db.execute('SELECT COUNT(*) as c FROM user_progress').fetchone()['c']
    correct = db.execute("SELECT COUNT(*) as c FROM user_progress WHERE result='correct'").fetchone()['c']
    incorrect = db.execute("SELECT COUNT(*) as c FROM user_progress WHERE result='incorrect'").fetchone()['c']
    skipped = db.execute("SELECT COUNT(*) as c FROM user_progress WHERE result='skipped'").fetchone()['c']

    theme_rows = db.execute(
        '''SELECT p.themes, up.result FROM user_progress up
           JOIN puzzles p ON p.id = up.puzzle_id
           WHERE p.themes != ''
        '''
    ).fetchall()

    theme_fails = Counter()
    theme_totals = Counter()
    theme_correct = Counter()
    for row in theme_rows:
        for theme in row['themes'].split():
            if theme not in ('middlegame', 'endgame', 'opening', 'crushing', 'advantage', 'master', 'masterVsMaster'):
                theme_totals[theme] += 1
                if row['result'] in ('incorrect', 'skipped'):
                    theme_fails[theme] += 1
                elif row['result'] == 'correct':
                    theme_correct[theme] += 1

    theme_breakdown = sorted(
        [{'theme': t, 'fails': theme_fails[t], 'total': theme_totals[t]} for t in theme_totals],
        key=lambda x: x['fails'],
        reverse=True
    )[:15]

    mastered_themes = [
        t for t in theme_totals
        if theme_totals[t] >= 5 and theme_correct[t] / theme_totals[t] >= 0.70
    ]

    accuracy = round(correct / total * 100) if total > 0 else 0

    return {
        'total': total,
        'correct': correct,
        'incorrect': incorrect,
        'skipped': skipped,
        'accuracy': accuracy,
        'theme_breakdown': theme_breakdown,
        'mastered_themes': sorted(mastered_themes),
    }


def get_session_results(puzzle_ids: list, results: list) -> list:
    db = get_db()
    result_map = {r['puzzle_id']: r for r in results}
    enriched = []
    for pid in puzzle_ids:
        puzzle = db.execute('SELECT * FROM puzzles WHERE id = ?', (pid,)).fetchone()
        if puzzle:
            r = result_map.get(pid, {'result': 'skipped', 'time_taken': 0})
            enriched.append({
                'puzzle_id': pid,
                'fen': puzzle['fen'],
                'themes': puzzle['themes'],
                'rating': puzzle['rating'],
                'result': r.get('result', 'skipped'),
                'time_taken': r.get('time_taken', 0),
                'correct_move': r.get('correct_move', ''),
            })
    return enriched


# ---------------------------------------------------------------------------
# Spaced repetition
# ---------------------------------------------------------------------------

def add_to_sr_queue(puzzle_ids: list):
    if not puzzle_ids:
        return
    db = get_db()
    for pid in puzzle_ids:
        existing = db.execute('SELECT fail_count FROM sr_queue WHERE puzzle_id = ?', (pid,)).fetchone()
        if existing:
            new_fail = existing['fail_count'] + 1
            new_due = min(4, new_fail)
            db.execute(
                'UPDATE sr_queue SET fail_count = ?, sessions_due = ? WHERE puzzle_id = ?',
                (new_fail, new_due, pid)
            )
        else:
            db.execute(
                'INSERT INTO sr_queue(puzzle_id, sessions_due, fail_count) VALUES(?, 1, 1)',
                (pid,)
            )
    db.commit()


def tick_sr_queue():
    db = get_db()
    db.execute('UPDATE sr_queue SET sessions_due = sessions_due - 1')
    db.commit()


def get_sr_due_puzzles(limit: int = 3) -> list:
    db = get_db()
    rows = db.execute(
        'SELECT puzzle_id FROM sr_queue WHERE sessions_due <= 0 ORDER BY fail_count DESC LIMIT ?',
        (limit,)
    ).fetchall()
    return [r['puzzle_id'] for r in rows]


def remove_from_sr_queue(puzzle_id: int):
    db = get_db()
    db.execute('DELETE FROM sr_queue WHERE puzzle_id = ?', (puzzle_id,))
    db.commit()


# ---------------------------------------------------------------------------
# Rating history
# ---------------------------------------------------------------------------

def record_rating_snapshot(rating: int):
    db = get_db()
    db.execute('INSERT INTO rating_history(rating) VALUES(?)', (rating,))
    db.commit()


def get_rating_history(limit: int = 20) -> list:
    db = get_db()
    rows = db.execute(
        '''SELECT rating, recorded_at FROM rating_history
           ORDER BY id DESC LIMIT ?''',
        (limit,)
    ).fetchall()
    return [{'rating': r['rating'], 'recorded_at': r['recorded_at']} for r in reversed(rows)]


# ---------------------------------------------------------------------------
# Training calendar
# ---------------------------------------------------------------------------

def get_training_calendar(days: int = 7) -> list:
    db = get_db()
    today = datetime.date.today()
    dates = [(today - datetime.timedelta(days=i)).isoformat() for i in range(days - 1, -1, -1)]

    rows = db.execute(
        '''SELECT date(attempted_at) as d FROM user_progress
           WHERE date(attempted_at) >= ?
           GROUP BY date(attempted_at)''',
        (dates[0],)
    ).fetchall()
    trained_dates = {r['d'] for r in rows}

    return [{'date': d, 'trained': d in trained_dates} for d in dates]
