"""
Scaruffi Checker — Flask web application.

Regole:
  rating >= 7  → tutte le tracce ammesse
  rating >= 6  → solo i 2 maggiori singoli  (6 incluso, 7 escluso)
  rating < 6   → nessuna traccia ammessa
"""

from flask import Flask, render_template, request, jsonify, Response, abort
import database as db
import musicbrainz_api as mb
try:
    import scraper
    _scraper_available = True
except ImportError:
    _scraper_available = False
import threading
import queue
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------------
# In-memory LRU cache for suggest endpoints
# ---------------------------------------------------------------------------

_suggest_cache = {}
_CACHE_MAX = 500


def _cache_get(key):
    return _suggest_cache.get(key)


def _cache_set(key, val):
    if len(_suggest_cache) >= _CACHE_MAX:
        _suggest_cache.pop(next(iter(_suggest_cache)))
    _suggest_cache[key] = val

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route('/')
def index():
    album_count = db.get_album_count()
    is_local = request.remote_addr in ('127.0.0.1', '::1')
    return render_template('index.html', album_count=album_count, is_local=is_local)


@app.route('/check', methods=['POST'])
def check():
    song   = request.form.get('song',   '').strip()
    artist = request.form.get('artist', '').strip()
    album  = request.form.get('album',  '').strip()

    if db.get_album_count() == 0:
        return jsonify({
            'status': 'error',
            'message': 'Database vuoto. Esegui prima la scansione dal menu in alto.'
        })

    # ── Modalità: solo album (nessun brano inserito) ──────────────────────────
    if not song:
        if not artist and not album:
            return jsonify({'status': 'error', 'message': 'Inserisci almeno un campo.'})
        return _check_album_only(artist, album)

    # ── Modalità: brano ───────────────────────────────────────────────────────
    return _check_song(song, artist, album)


def _check_album_only(artist, album_query):
    """Cerca l'album nel DB Scaruffi e restituisce il voto."""
    match = None

    if artist and album_query:
        match = db.find_album(artist, album_query)
    elif artist:
        results = db.search_artists(artist, limit=1)
        if results:
            match = db.find_album(results[0]['artist'], results[0]['top_album'])
    elif album_query:
        match = db.find_album_by_title(album_query)

    if not match:
        return jsonify({
            'status': 'not_in_scaruffi',
            'message': f'Album{" di " + artist if artist else ""}'
                       f'{" "" + album_query + """ if album_query else ""} '
                       'non trovato nel database Scaruffi.'
        })

    rating = match['rating']
    if rating < 6:
        status = 'no';  reason = f'Rating {rating}/10 < 6 — album sotto soglia, nessuna traccia ammessa.'
    elif rating < 7:
        status = 'partial'; reason = f'Rating {rating}/10 (fascia 6–<7) — solo i 2 maggiori singoli.'
    else:
        status = 'yes'; reason = f'Rating {rating}/10 ≥ 7 — tutte le tracce ammesse.'
    return jsonify({
        'mode':            'album',
        'status':          status,
        'scaruffi_artist': match['artist'],
        'scaruffi_album':  match['album'],
        'scaruffi_year':   match.get('year'),
        'rating':          rating,
        'reason':          reason,
    })


def _check_song(song, artist, album_hint):
    """Controlla se un brano è ammesso con double-check sull'album originale."""

    # Step 1 — MusicBrainz con double-check: album originale in studio,
    #          esclude compilazioni / live / reissue, prende il più vecchio.
    recordings = mb.find_recording(song, artist or None)

    if not recordings:
        return jsonify({
            'status': 'not_found',
            'message': (f'Brano "{song}"' + (f' di {artist}' if artist else '') +
                        ' non trovato su MusicBrainz. '
                        'Prova a verificare titolo o artista.')
        })

    # Step 2 — abbina al DB Scaruffi
    matched_rec    = None
    scaruffi_match = None
    for rec in recordings[:5]:
        match = db.find_album(rec['artist'], rec['album'])
        if match:
            matched_rec    = rec
            scaruffi_match = match
            break

    if not scaruffi_match:
        best = recordings[0]
        return jsonify({
            'status':     'not_in_scaruffi',
            'song':       best['title'],
            'artist':     best['artist'],
            'album':      best['album'],
            'album_year': best.get('year'),
            'message':    (f'Album originale: "{best["album"]}" '
                           f'({best.get("year","?")}) di {best["artist"]}. '
                           'Non presente nel database Scaruffi.')
        })

    # Step 3 — applica regole originali
    rating     = scaruffi_match['rating']
    rec_title  = matched_rec['title']
    rec_artist = matched_rec['artist']
    album_year = matched_rec.get('year')

    base = {
        'mode':            'song',
        'song':            rec_title,
        'artist':          rec_artist,
        'album':           matched_rec['album'],
        'album_year':      album_year,
        'scaruffi_artist': scaruffi_match['artist'],
        'scaruffi_album':  scaruffi_match['album'],
        'scaruffi_year':   scaruffi_match.get('year'),
        'rating':          rating,
        'match_score':     scaruffi_match['match_score'],
    }

    if rating < 6:
        return jsonify({**base, 'status': 'no',
                        'reason': f'Rating {rating}/10 < 6: album non qualificato.'})

    if rating >= 7:
        return jsonify({**base, 'status': 'yes',
                        'reason': f'Rating {rating}/10 ≥ 7: tutte le tracce ammesse.'})

    # 6 ≤ rating < 7 → solo i 2 maggiori singoli (6 incluso, 7 escluso)
    is_single, _ = mb.is_track_a_single(rec_title, rec_artist)
    if not is_single:
        return jsonify({**base, 'status': 'no',
                        'reason': (f'Rating {rating}/10 (fascia 6–<7): '
                                   'solo i 2 maggiori singoli. '
                                   'Questo brano non è un singolo.')})

    top_singles  = mb.get_top_2_singles(rec_artist, album_year=album_year)
    top_2_titles = [s['title'].lower() for s in top_singles]
    from difflib import SequenceMatcher
    song_lower = rec_title.lower()
    in_top_2 = (song_lower in top_2_titles or
                any(SequenceMatcher(None, song_lower, t).ratio() >= 0.82
                    for t in top_2_titles))

    if in_top_2:
        return jsonify({**base, 'status': 'yes', 'top_2_singles': top_singles,
                        'reason': (f'Rating {rating}/10 (fascia 6–<7): '
                                   'è uno dei 2 maggiori singoli. Ammesso.')})
    return jsonify({**base, 'status': 'no', 'top_2_singles': top_singles,
                    'reason': (f'Rating {rating}/10 (fascia 6–<7): '
                               f'singolo non nei top 2 '
                               f'({", ".join(s["title"] for s in top_singles) or "N/D"}).')})


def localhost_only():
    """Blocca accesso se non viene da localhost."""
    if request.remote_addr not in ('127.0.0.1', '::1'):
        abort(403)

@app.route('/api/top-albums')
def api_top_albums():
    decade = request.args.get('decade', type=int)
    min_r  = request.args.get('min_rating', 6.0, type=float)
    limit  = request.args.get('limit', 20, type=int)
    return jsonify(db.get_top_albums(decade=decade, min_rating=min_r, limit=min(limit, 100)))

@app.route('/api/stats')
def api_stats():
    return jsonify(db.get_stats())

@app.route('/api/random')
def api_random():
    min_r = request.args.get('min_rating', 7.0, type=float)
    result = db.get_random_album(min_rating=min_r)
    if not result:
        return jsonify({'error': 'Nessun album trovato'}), 404
    return jsonify(result)

@app.route('/api/similar')
def api_similar():
    artist = request.args.get('artist', '').strip()
    rating = request.args.get('rating', 7.0, type=float)
    if not artist:
        return jsonify([])
    return jsonify(db.get_similar_artists(artist, rating))

@app.route('/api/hall-of-fame')
def api_hall_of_fame():
    return jsonify(db.get_hall_of_fame())

@app.route('/api/decade-champions')
def api_decade_champions():
    return jsonify(db.get_decade_champions())

@app.route('/classifica')
def classifica():
    return render_template('classifica.html', album_count=db.get_album_count())

@app.route('/stats')
def stats_page():
    return render_template('stats.html', album_count=db.get_album_count())

@app.route('/scopri')
def scopri():
    return render_template('scopri.html', album_count=db.get_album_count())


@app.route('/quiz')
def quiz():
    return render_template('quiz.html', album_count=db.get_album_count())


@app.route('/scrape')
def scrape_page():
    localhost_only()
    return render_template('scrape.html')


@app.route('/scrape/run')
def scrape_run():
    localhost_only()
    """SSE: scansione rapida (solo pagine ratings ≥7)."""
    if not _scraper_available:
        abort(503)
    return _sse_scrape(lambda cb: scraper.scrape_all(progress_cb=cb))


@app.route('/scrape/deep')
def scrape_deep_run():
    localhost_only()
    """SSE: scansione profonda — tutti i 9500 artisti recensiti."""
    import scraper_deep
    return _sse_scrape(lambda cb: scraper_deep.scrape_deep(progress_cb=cb, skip_existing=True))


def _sse_scrape(task_fn):
    """Avvia task_fn in un thread e lo streamma come Server-Sent Events."""
    q = queue.Queue()

    def run():
        def cb(msg):
            q.put(str(msg))
        try:
            total = task_fn(cb)
            q.put(f'__DONE__{total}')
        except Exception as e:
            q.put(f'__ERROR__{e}')

    t = threading.Thread(target=run, daemon=True)
    t.start()

    def generate():
        while True:
            msg = q.get()
            if msg.startswith('__DONE__'):
                total = msg[len('__DONE__'):]
                yield f"data: DONE:{total}\n\n"
                break
            elif msg.startswith('__ERROR__'):
                err = msg[len('__ERROR__'):]
                yield f"data: ERROR:{err}\n\n"
                break
            else:
                yield f"data: {msg}\n\n"

    return Response(generate(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache',
                             'X-Accel-Buffering': 'no'})


@app.route('/track-verify')
def track_verify():
    """
    Verifica live: dato un brano (e opzionalmente artista),
    restituisce l'album originale da MusicBrainz + rating Scaruffi.
    """
    song   = request.args.get('q',      '').strip()
    artist = request.args.get('artist', '').strip()
    if len(song) < 2:
        return jsonify({'status': 'empty'})

    recordings = mb.find_recording(song, artist or None)
    if not recordings:
        return jsonify({'status': 'not_found'})

    # Cerca corrispondenza Scaruffi tra i primi 5 risultati
    for rec in recordings[:5]:
        match = db.find_album(rec['artist'], rec['album'])
        if match:
            return jsonify({
                'status':       'found',
                'song':         rec['title'],
                'artist':       rec['artist'],
                'album':        rec['album'],
                'year':         rec.get('year'),
                'in_scaruffi':  True,
                'scaruffi_album': match['album'],
                'scaruffi_year':  match.get('year'),
                'rating':       match['rating'],
                'url':          match.get('url'),
            })

    # Nessun match Scaruffi — restituisce comunque i dati MB
    best = recordings[0]
    return jsonify({
        'status':      'found',
        'song':        best['title'],
        'artist':      best['artist'],
        'album':       best['album'],
        'year':        best.get('year'),
        'in_scaruffi': False,
        'rating':      None,
    })


@app.route('/song-suggest')
def song_suggest():
    """Live autocomplete for song title via MusicBrainz."""
    q = request.args.get('q', '').strip()
    artist = request.args.get('artist', '').strip()
    if len(q) < 2:
        return jsonify([])

    recordings = mb.find_recording(q, artist or None)

    # For each unique (song, artist) pair, try ALL album variants to find
    # any Scaruffi match — same logic used in /check
    seen_pairs = set()
    results = []

    for rec in recordings[:15]:
        pair = (rec['artist'].lower(), rec['title'].lower())
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)

        # Try to find this artist+album in Scaruffi (may need several tries
        # if MusicBrainz returns slightly different album names)
        match = db.find_album(rec['artist'], rec['album'])

        # If no match, try with a lower threshold as fallback
        if not match:
            match = db.find_album(rec['artist'], rec['album'], threshold=0.30)

        results.append({
            'title': rec['title'],
            'artist': rec['artist'],
            'album': rec['album'],
            'year': rec.get('year'),
            'in_scaruffi': match is not None,
            'rating': match['rating'] if match else None,
        })

    # Sort: Scaruffi matches first, then by rating desc
    results.sort(key=lambda x: (0 if x['in_scaruffi'] else 1,
                                 -(x['rating'] or 0)))
    return jsonify(results[:8])


@app.route('/artist-suggest')
def artist_suggest():
    """Live autocomplete: artisti dal DB Scaruffi."""
    q = request.args.get('q', '').strip()
    if len(q) < 2:
        return jsonify([])
    cache_key = f"a:{q}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return jsonify(cached)
    result = db.search_artists(q, limit=8)
    _cache_set(cache_key, result)
    return jsonify(result)


@app.route('/album-suggest')
def album_suggest():
    """Live autocomplete: album dal DB Scaruffi, con filtro artista opzionale.
    Se q è vuoto ma artist è presente, restituisce tutti gli album dell'artista."""
    q      = request.args.get('q', '').strip()
    artist = request.args.get('artist', '').strip()
    # Richiedi almeno 2 char SOLO se non c'è artista specificato
    if len(q) < 2 and not artist:
        return jsonify([])
    cache_key = f"al:{q}:{artist}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return jsonify(cached)
    limit = 30 if (not q and artist) else 8
    result = db.search_albums(q, artist_filter=artist or None, limit=limit)
    _cache_set(cache_key, result)
    return jsonify(result)


@app.route('/artist-albums')
def artist_albums():
    """Pre-fetch: restituisce TUTTI gli album di un artista in un'unica chiamata."""
    artist = request.args.get('artist', '').strip()
    if not artist:
        return jsonify([])
    return jsonify(db.search_albums('', artist_filter=artist, limit=50))


@app.route('/status')
def status():
    return jsonify({'album_count': db.get_album_count()})


# ---------------------------------------------------------------------------
# Deep Scan Progress (public)
# ---------------------------------------------------------------------------

@app.route('/progress')
def progress_page():
    return render_template('progress.html')


@app.route('/progress/data')
def progress_data():
    """JSON snapshot del deep scan — chiamato ogni 2s dalla pagina progress."""
    import sqlite3 as _sq
    import re as _re
    import os as _os

    DB_P = _os.path.join(_os.path.dirname(__file__), 'scaruffi.db')
    LOG  = _os.path.join(_os.path.dirname(__file__), 'deep_scrape.log')
    TOTAL_ARTISTS = 7664

    try:
        with _sq.connect(DB_P) as conn:
            album_count  = conn.execute('SELECT COUNT(*) FROM albums').fetchone()[0]
            artist_count = conn.execute(
                'SELECT COUNT(DISTINCT url) FROM albums WHERE url IS NOT NULL'
            ).fetchone()[0]
    except Exception:
        album_count = artist_count = 0

    last_artist = ''
    nuovi = 0
    done  = False
    try:
        with open(LOG, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()
        # Controlla se l'ultima scansione è completata (Done. presente)
        done = any('Done.' in l for l in lines)
        # Cerca ultimo artista e contatore nuovi (dal fondo)
        for raw_line in reversed(lines):
            line = raw_line.strip()
            if not line:
                continue
            m = _re.search(r'tot\. nuovi:\s*(\d+)', line)
            if m:
                nuovi = int(m.group(1))
            m2 = _re.match(r'\s{2}(.+?):\s+\d+ album', raw_line)
            if m2 and not last_artist:
                last_artist = m2.group(1).strip()
            if nuovi and last_artist:
                break
    except Exception:
        pass

    pct = 100.0 if done else round(artist_count / TOTAL_ARTISTS * 100, 1)
    display_artists = TOTAL_ARTISTS if done else artist_count
    return jsonify({
        'album_count':   album_count,
        'artist_count':  display_artists,
        'total_artists': TOTAL_ARTISTS,
        'pct':           pct,
        'last_artist':   last_artist,
        'nuovi':         nuovi,
        'done':          done,
    })


if __name__ == '__main__':
    db.init_db()
    app.run(debug=True, port=5000)
