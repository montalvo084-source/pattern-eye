import os
import json
import time
import random
import datetime

import chess
import anthropic
from dotenv import load_dotenv
from flask import (
    Flask, render_template, redirect, url_for,
    session, request, jsonify, abort
)

import database as db

load_dotenv()

THEME_TIPS = {
    'fork':             "One of your pieces can move to a square where it attacks TWO of their pieces at once. They can only save one — you take the other.",
    'pin':              "One of their pieces is frozen. If it moves, they lose something more valuable sitting behind it. Attack that frozen piece.",
    'skewer':           "Attack their most valuable piece. When it runs away, you capture the piece that was hiding behind it.",
    'backrank':         "Their king is stuck on the last row, blocked by its own pawns. A rook or queen sliding to that row ends the game.",
    'matein1':          "You can end the game right now with one move. Their king will have no safe square to go to.",
    'matein2':          "You can force checkmate in two moves. Make a threat so powerful they can only delay, not stop it.",
    'discoveredattack': "Move one of your pieces out of the way — doing so will reveal a powerful attack from the piece that was behind it.",
    'hangingpiece':     "One of their pieces is sitting there completely undefended. No piece is protecting it. Take it for free.",
    'trappedpiece':     "One of their pieces has no safe square to run to. Every square it could go to is covered. Attack it.",
    'deflection':       "One of their pieces is doing a very important job — guarding something critical. Force it to move away.",
}
GENERIC_TIPS = [
    "Scan for any of their pieces that have no protection. Free material is the easiest win.",
    "Picture the most damaging thing you could do to them. Is any of your pieces one move away from doing it?",
    "Look at your most powerful piece. Where could it move to cause the biggest problem?",
    "Start with checks — does any check lead to winning a piece or forcing mate?",
]

def get_coach_message(themes: str) -> str:
    for theme in themes.lower().split():
        if theme in THEME_TIPS:
            return THEME_TIPS[theme]
    return random.choice(GENERIC_TIPS)


app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-change-me')

with app.app_context():
    db.init_db(app)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def moves_match(user_uci: str, expected_uci: str) -> bool:
    if user_uci == expected_uci:
        return True
    # Accept bare 4-char UCI when expected has a promotion suffix (always queen)
    if len(expected_uci) == 5 and user_uci == expected_uci[:4]:
        return True
    return False


def _get_next_url(training: dict) -> str:
    idx = training['current_index']
    ids = training['puzzle_ids']
    if idx >= len(ids):
        weak = db.recalculate_weak_themes()
        db.update_weak_themes(weak)
        correct_count = sum(1 for r in training['results'] if r['result'] == 'correct')
        db.update_session_streak(correct_count)

        # Spaced repetition: queue wrong/skipped puzzles for future sessions
        wrong_ids = [r['puzzle_id'] for r in training['results']
                     if r['result'] in ('incorrect', 'skipped')]
        db.add_to_sr_queue(wrong_ids)

        # Record rating snapshot for progress chart
        stats = db.get_user_stats()
        db.record_rating_snapshot(stats['current_rating'])

        return url_for('session_summary')
    return url_for('show_puzzle', puzzle_id=ids[idx])


def _get_ai_explanation(puzzle, moves: list, move_step: int, correct_uci: str) -> str:
    api_key = os.environ.get('ANTHROPIC_API_KEY', '')
    if not api_key:
        return ''
    try:
        board = chess.Board(puzzle['fen'])
        for m in moves[:move_step]:
            board.push_uci(m)
        correct_move_obj = chess.Move.from_uci(correct_uci[:4] if len(correct_uci) == 5 else correct_uci)
        # For promotions, attach the promotion piece
        if len(correct_uci) == 5:
            promo_map = {'q': chess.QUEEN, 'r': chess.ROOK, 'b': chess.BISHOP, 'n': chess.KNIGHT}
            correct_move_obj = chess.Move(
                chess.parse_square(correct_uci[:2]),
                chess.parse_square(correct_uci[2:4]),
                promotion=promo_map.get(correct_uci[4], chess.QUEEN)
            )
        san_move = board.san(correct_move_obj)
        fen_for_prompt = board.fen()
        themes = puzzle['themes'].replace(' ', ', ')

        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model='claude-haiku-4-5-20251001',
            max_tokens=150,
            system=(
                'You are a chess coach explaining a move to a beginner. '
                'Write exactly 2 sentences. '
                'Sentence 1: say what the move does in plain English — what piece moves, what it attacks or threatens. '
                'Sentence 2: say why the opponent cannot stop it or what they lose. '
                'Never use chess jargon like fork, pin, skewer, discovered attack, deflection, zwischenzug. '
                'Write like you are texting a friend. Short words. No fluff.'
            ),
            messages=[{
                'role': 'user',
                'content': f'Position (FEN): {fen_for_prompt}\nCorrect move: {san_move}\nWhy does this move win?'
            }]
        )
        return msg.content[0].text
    except Exception:
        return ''


def _get_hint(puzzle, moves: list, move_step: int) -> str:
    api_key = os.environ.get('ANTHROPIC_API_KEY', '')
    if not api_key:
        return get_coach_message(puzzle['themes'] or '')
    try:
        board = chess.Board(puzzle['fen'])
        for m in moves[:move_step]:
            board.push_uci(m)
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model='claude-haiku-4-5-20251001',
            max_tokens=80,
            system=(
                'You are a chess coach giving a hint to a beginner. '
                'One sentence only. '
                'Say which piece to focus on (use its name: king, queen, rook, bishop, knight, or pawn) and what kind of opportunity to look for. '
                'Do NOT reveal the move or the destination square. '
                'No chess jargon. Write like you are texting a friend.'
            ),
            messages=[{'role': 'user', 'content':
                f'Position (FEN): {board.fen()}\nGive a one-sentence hint.'}]
        )
        return msg.content[0].text
    except Exception:
        return get_coach_message(puzzle['themes'] or '')


def _get_ai_praise(puzzle, moves: list, move_step: int, correct_uci: str) -> str:
    api_key = os.environ.get('ANTHROPIC_API_KEY', '')
    if not api_key:
        return "Yes! That was the winning move — you spotted it perfectly."
    try:
        board = chess.Board(puzzle['fen'])
        for m in moves[:move_step]:
            board.push_uci(m)
        uci_clean = correct_uci[:4] if len(correct_uci) == 5 else correct_uci
        move_obj = board.parse_uci(uci_clean)
        if len(correct_uci) == 5:
            promo_map = {'q': chess.QUEEN, 'r': chess.ROOK, 'b': chess.BISHOP, 'n': chess.KNIGHT}
            move_obj = chess.Move(
                chess.parse_square(correct_uci[:2]),
                chess.parse_square(correct_uci[2:4]),
                promotion=promo_map.get(correct_uci[4], chess.QUEEN)
            )
        san_move = board.san(move_obj)
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model='claude-haiku-4-5-20251001',
            max_tokens=120,
            system=(
                'You are an enthusiastic chess coach. The player just found the right move! '
                'Write exactly 2 sentences. '
                'Sentence 1: Celebrate genuinely — be warm and specific to what they just did, not a generic "Great job!". '
                'Sentence 2: Explain in plain English what made their move so powerful — what it attacked or threatened, and why the opponent had no good answer. '
                'No chess jargon. Write like you are texting a friend who just did something impressive.'
            ),
            messages=[{
                'role': 'user',
                'content': f'Position (FEN): {board.fen()}\nThe player played: {san_move}\nCelebrate and explain why this move wins.'
            }]
        )
        return msg.content[0].text
    except Exception:
        return "Yes! That was the winning move — you spotted it perfectly."


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route('/')
def index():
    stats = db.get_user_stats()
    today = datetime.date.today().isoformat()
    calendar = db.get_training_calendar(7)
    return render_template('index.html', stats=stats, today=today, calendar=calendar)


@app.route('/session')
def start_session():
    stats = db.get_user_stats()
    weak_themes = json.loads(stats['weak_themes'])
    puzzle_ids = db.get_puzzles_for_session(stats['current_rating'], weak_themes)

    if not puzzle_ids:
        return render_template('index.html', stats=stats, error='No puzzles found. Run the import script first.')

    # Spaced repetition: tick queue and prepend due puzzles (cap at 1 for 4-puzzle sessions)
    db.tick_sr_queue()
    sr_ids = db.get_sr_due_puzzles(1)
    # Avoid duplicates between SR ids and fresh puzzle ids
    fresh_ids = [pid for pid in puzzle_ids if pid not in sr_ids]
    combined_ids = sr_ids + fresh_ids

    session['training'] = {
        'puzzle_ids': combined_ids,
        'current_index': 0,
        'results': [],
        'move_steps': {},
        'start_times': {},
        'retried': [],
    }
    return redirect(url_for('show_puzzle', puzzle_id=puzzle_ids[0]))


@app.route('/puzzle/<int:puzzle_id>')
def show_puzzle(puzzle_id):
    training = session.get('training')
    if not training:
        return redirect(url_for('index'))

    puzzle = db.get_puzzle(puzzle_id)
    if not puzzle:
        abort(404)

    moves = puzzle['moves'].split()
    current_index = training['current_index']

    # Only set start_time and move_step on first visit (handles Back button)
    if str(puzzle_id) not in training['start_times']:
        training['start_times'][str(puzzle_id)] = int(time.time())
    if str(puzzle_id) not in training['move_steps']:
        training['move_steps'][str(puzzle_id)] = 1
    session.modified = True

    # Determine whose turn it is after the setup move (moves[0])
    try:
        board = chess.Board(puzzle['fen'])
        board.push_uci(moves[0])
    except Exception:
        # Invalid puzzle data — skip it and move to the next one
        training['current_index'] += 1
        session.modified = True
        return redirect(_get_next_url(training))
    orientation = 'white' if board.turn == chess.WHITE else 'black'
    side_to_move = 'White' if board.turn == chess.WHITE else 'Black'

    return render_template(
        'puzzle.html',
        puzzle=puzzle,
        moves=moves,
        orientation=orientation,
        side_to_move=side_to_move,
        puzzle_number=current_index + 1,
        total_puzzles=len(training['puzzle_ids']),
        coach_message=get_coach_message(puzzle['themes']),
    )


@app.route('/puzzle/<int:puzzle_id>/attempt', methods=['POST'])
def attempt_puzzle(puzzle_id):
    training = session.get('training')
    if not training:
        return jsonify({'error': 'no session'}), 400

    data = request.get_json()
    if not data or 'move' not in data:
        return jsonify({'error': 'missing move'}), 400

    user_move = data['move'].lower().strip()
    puzzle = db.get_puzzle(puzzle_id)
    if not puzzle:
        return jsonify({'error': 'puzzle not found'}), 404

    moves = puzzle['moves'].split()
    move_step = training['move_steps'].get(str(puzzle_id), 1)

    if move_step >= len(moves):
        return jsonify({'error': 'puzzle already complete'}), 400

    expected_move = moves[move_step]

    # ---- Incorrect ----
    if not moves_match(user_move, expected_move):
        start_time = training['start_times'].get(str(puzzle_id), int(time.time()))
        time_taken = int(time.time()) - start_time

        db.record_attempt(puzzle_id, 'incorrect', time_taken)
        db.update_user_stats(rating_delta=-5, solved_delta=0)

        training['results'].append({
            'puzzle_id': puzzle_id,
            'result': 'incorrect',
            'time_taken': time_taken,
            'correct_move': expected_move,
        })
        training['current_index'] += 1
        session.modified = True

        explanation = _get_ai_explanation(puzzle, moves, move_step, expected_move)

        # Build a human-readable move label (e.g. "Knight to e6")
        try:
            label_board = chess.Board(puzzle['fen'])
            for m in moves[:move_step]:
                label_board.push_uci(m)
            uci_for_label = expected_move[:4] if len(expected_move) == 5 else expected_move
            move_obj = label_board.parse_uci(uci_for_label)
            if len(expected_move) == 5:
                promo_map = {'q': chess.QUEEN, 'r': chess.ROOK, 'b': chess.BISHOP, 'n': chess.KNIGHT}
                move_obj = chess.Move(
                    chess.parse_square(expected_move[:2]),
                    chess.parse_square(expected_move[2:4]),
                    promotion=promo_map.get(expected_move[4], chess.QUEEN)
                )
            piece = label_board.piece_at(move_obj.from_square)
            piece_names = {chess.KING: 'King', chess.QUEEN: 'Queen', chess.ROOK: 'Rook',
                           chess.BISHOP: 'Bishop', chess.KNIGHT: 'Knight', chess.PAWN: 'Pawn'}
            piece_name = piece_names.get(piece.piece_type, '') if piece else ''
            dest = chess.square_name(move_obj.to_square).upper()
            correct_move_label = f'{piece_name} to {dest}' if piece_name else dest
        except Exception:
            correct_move_label = expected_move.upper()

        # Re-queue for end of session (once per puzzle)
        retried = training.get('retried', [])
        re_queued = False
        if puzzle_id not in retried:
            training['puzzle_ids'].append(puzzle_id)
            retried.append(puzzle_id)
            training['retried'] = retried
            re_queued = True
        session.modified = True

        return jsonify({
            'correct': False,
            'correct_move': expected_move,
            'correct_move_label': correct_move_label,
            'explanation': explanation,
            're_queued': re_queued,
            'next_url': _get_next_url(training),
        })

    # ---- Correct ----
    next_computer_idx = move_step + 1
    next_user_idx = move_step + 2

    if next_computer_idx < len(moves):
        # Multi-step: there's a computer response move to animate
        training['move_steps'][str(puzzle_id)] = next_user_idx
        session.modified = True
        return jsonify({
            'correct': True,
            'complete': False,
            'animate_move': moves[next_computer_idx],
        })

    # Puzzle fully solved
    start_time = training['start_times'].get(str(puzzle_id), int(time.time()))
    time_taken = int(time.time()) - start_time

    db.record_attempt(puzzle_id, 'correct', time_taken)
    db.update_user_stats(rating_delta=10, solved_delta=1)

    training['results'].append({
        'puzzle_id': puzzle_id,
        'result': 'correct',
        'time_taken': time_taken,
    })
    training['current_index'] += 1
    session.modified = True

    db.remove_from_sr_queue(puzzle_id)
    praise = _get_ai_praise(puzzle, moves, move_step, expected_move)

    return jsonify({
        'correct': True,
        'complete': True,
        'praise': praise,
        'next_url': _get_next_url(training),
    })


@app.route('/puzzle/<int:puzzle_id>/skip', methods=['POST'])
def skip_puzzle(puzzle_id):
    training = session.get('training')
    if not training:
        return jsonify({'error': 'no session'}), 400

    start_time = training['start_times'].get(str(puzzle_id), int(time.time()))
    time_taken = int(time.time()) - start_time

    db.record_attempt(puzzle_id, 'skipped', time_taken)

    training['results'].append({
        'puzzle_id': puzzle_id,
        'result': 'skipped',
        'time_taken': time_taken,
    })
    training['current_index'] += 1
    session.modified = True

    return jsonify({'next_url': _get_next_url(training)})


@app.route('/puzzle/<int:puzzle_id>/hint')
def hint_puzzle(puzzle_id):
    training = session.get('training')
    if not training:
        return jsonify({'error': 'no session'}), 400
    puzzle = db.get_puzzle(puzzle_id)
    if not puzzle:
        return jsonify({'error': 'not found'}), 404
    moves = puzzle['moves'].split()
    move_step = training['move_steps'].get(str(puzzle_id), 1)
    return jsonify({'hint': _get_hint(puzzle, moves, move_step)})


@app.route('/session/summary')
def session_summary():
    training = session.get('training')
    if not training:
        return redirect(url_for('index'))

    results = db.get_session_results(training['puzzle_ids'], training['results'])
    stats = db.get_user_stats()
    correct_count = sum(1 for r in training['results'] if r['result'] == 'correct')
    incorrect_count = sum(1 for r in training['results'] if r['result'] == 'incorrect')

    session.pop('training', None)

    if correct_count == 4:
        session_message = "Perfect session. Your pattern recognition is building."
    elif correct_count >= 3:
        session_message = "Solid work. The ones you missed are the ones you'll remember."
    else:
        session_message = "Tough session — but studying hard puzzles is how you improve fastest."

    return render_template(
        'summary.html',
        results=results,
        stats=stats,
        correct_count=correct_count,
        incorrect_count=incorrect_count,
        total=len(training.get('puzzle_ids', [])) or 4,
        session_message=session_message,
    )


@app.route('/dashboard')
def dashboard():
    stats = db.get_user_stats()
    lifetime = db.get_lifetime_stats()
    weak_themes = json.loads(stats['weak_themes'])
    today = datetime.date.today().isoformat()
    calendar = db.get_training_calendar(7)
    rating_history = db.get_rating_history(20)
    return render_template(
        'dashboard.html',
        stats=stats,
        lifetime=lifetime,
        weak_themes=weak_themes,
        today=today,
        calendar=calendar,
        rating_history=rating_history,
    )


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(debug=os.environ.get('FLASK_DEBUG', 'false').lower() == 'true', port=port)
