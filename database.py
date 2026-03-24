import sqlite3
import re
import os
import json
from difflib import SequenceMatcher
from functools import lru_cache

DB_PATH = os.path.join(os.path.dirname(__file__), 'scaruffi.db')

# In-memory LRU cache for find_album results.
# Keyed on (artist_query, album_query, threshold).
_find_album_cache = {}
_FIND_ALBUM_CACHE_MAX = 512


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS albums (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                artist TEXT NOT NULL,
                artist_norm TEXT NOT NULL,
                album_title TEXT NOT NULL,
                album_norm TEXT NOT NULL,
                year INTEGER,
                rating REAL NOT NULL,
                url TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(artist_norm, album_norm)
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS mb_cache (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # FTS5 virtual table over artist_norm + album_norm.
        # content= links it to the albums table so no data is duplicated.
        conn.execute('''
            CREATE VIRTUAL TABLE IF NOT EXISTS albums_fts
            USING fts5(
                artist_norm,
                album_norm,
                content=albums,
                content_rowid=id
            )
        ''')

        # Triggers to keep the FTS index in sync with the albums table.
        conn.execute('''
            CREATE TRIGGER IF NOT EXISTS albums_ai
            AFTER INSERT ON albums BEGIN
                INSERT INTO albums_fts(rowid, artist_norm, album_norm)
                VALUES (new.id, new.artist_norm, new.album_norm);
            END
        ''')
        conn.execute('''
            CREATE TRIGGER IF NOT EXISTS albums_ad
            AFTER DELETE ON albums BEGIN
                INSERT INTO albums_fts(albums_fts, rowid, artist_norm, album_norm)
                VALUES ('delete', old.id, old.artist_norm, old.album_norm);
            END
        ''')
        conn.execute('''
            CREATE TRIGGER IF NOT EXISTS albums_au
            AFTER UPDATE ON albums BEGIN
                INSERT INTO albums_fts(albums_fts, rowid, artist_norm, album_norm)
                VALUES ('delete', old.id, old.artist_norm, old.album_norm);
                INSERT INTO albums_fts(rowid, artist_norm, album_norm)
                VALUES (new.id, new.artist_norm, new.album_norm);
            END
        ''')

        conn.commit()


def _populate_fts():
    """Rebuild the FTS index from the albums table (run once after migration)."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("INSERT INTO albums_fts(albums_fts) VALUES ('rebuild')")
        conn.commit()


def _normalize(text):
    """Normalize text for fuzzy matching."""
    text = text.lower().strip()
    text = re.sub(r'^(the|a|an|i|gli|le|la|lo)\s+', '', text)
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def _fts_query(token):
    """Escape a token for an FTS5 query (prefix search)."""
    # FTS5 special characters that must be escaped or dropped.
    token = re.sub(r'["\-\^*]', '', token)
    token = token.strip()
    return token


def _build_fts_expression(artist_norm, album_norm):
    """
    Build an FTS5 MATCH expression that searches for individual tokens
    from the normalized artist and album strings.  We use prefix search
    on every significant token so that partial words still match.
    """
    tokens = set((artist_norm + ' ' + album_norm).split())
    # Drop very short tokens to avoid broad FTS hits.
    tokens = {t for t in tokens if len(t) > 1}
    if not tokens:
        return None
    parts = ' OR '.join(f'"{_fts_query(t)}"*' for t in tokens if _fts_query(t))
    return parts if parts else None


def insert_album(artist, album_title, year, rating, url=None):
    # Invalidate the find_album cache whenever the data changes.
    _find_album_cache.clear()
    with sqlite3.connect(DB_PATH) as conn:
        try:
            conn.execute('''
                INSERT OR REPLACE INTO albums
                (artist, artist_norm, album_title, album_norm, year, rating, url)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (artist, _normalize(artist), album_title, _normalize(album_title),
                  year, rating, url))
            conn.commit()
        except Exception:
            pass


def find_album(artist_query, album_query, threshold=0.45):
    """
    Find the best matching album using a two-stage approach:
      1. FTS5 index to narrow the full corpus down to ~20 candidates.
      2. SequenceMatcher scoring applied only to those candidates.
    Results are cached in a bounded in-memory dict.
    """
    cache_key = (artist_query.lower(), album_query.lower(), threshold)
    if cache_key in _find_album_cache:
        return _find_album_cache[cache_key]

    artist_norm = _normalize(artist_query)
    album_norm = _normalize(album_query)

    fts_expr = _build_fts_expression(artist_norm, album_norm)

    with sqlite3.connect(DB_PATH) as conn:
        if fts_expr:
            try:
                rows = conn.execute(
                    '''
                    SELECT a.artist, a.album_title, a.year, a.rating, a.url,
                           a.artist_norm, a.album_norm
                    FROM albums a
                    JOIN albums_fts f ON f.rowid = a.id
                    WHERE albums_fts MATCH ?
                    ORDER BY rank
                    LIMIT 20
                    ''',
                    (fts_expr,)
                ).fetchall()
            except sqlite3.OperationalError:
                # FTS table missing or query error — fall back to full scan.
                rows = []
        else:
            rows = []

        # If FTS returned nothing (new DB, bad query, etc.) fall back to a
        # simple LIKE scan which is much cheaper than loading all 7600 rows
        # when FTS is available, but safe as a degraded path.
        if not rows:
            like_artist = f'%{artist_norm}%'
            like_album = f'%{album_norm}%'
            rows = conn.execute(
                '''
                SELECT artist, album_title, year, rating, url,
                       artist_norm, album_norm
                FROM albums
                WHERE artist_norm LIKE ? OR album_norm LIKE ?
                LIMIT 40
                ''',
                (like_artist, like_album)
            ).fetchall()

    if not rows:
        _find_album_cache[cache_key] = None
        return None

    best_score = 0
    best_match = None

    artist_words = set(artist_norm.split())

    for artist, album_title, year, rating, url, a_norm, al_norm in rows:
        db_words = set(a_norm.split())

        # ── Artist matching ──────────────────────────────────────────────────
        # Per nomi corti (≤ 4 car) usa corrispondenza parola esatta —
        # ratio di sequenza è troppo rumorosa su stringhe brevi.
        if len(artist_norm) <= 4:
            a_score = 1.0 if artist_words & db_words else 0.0
        else:
            a_score = SequenceMatcher(None, a_norm, artist_norm).ratio()
            # Boost se una stringa contiene l'altra (es. "Beatles" in "The Beatles")
            if artist_norm in a_norm or a_norm in artist_norm:
                a_score = max(a_score, 0.85)

        # GATE duro: l'artista deve assomigliare almeno al 45%
        if a_score < 0.45:
            continue

        al_score = SequenceMatcher(None, al_norm, album_norm).ratio()

        score = a_score * 0.45 + al_score * 0.55

        if score > best_score:
            best_score = score
            best_match = {
                'artist': artist,
                'album': album_title,
                'year': year,
                'rating': rating,
                'url': url,
                'match_score': round(score, 3),
            }

    result = best_match if best_score >= threshold else None

    # Evict oldest entry when the cache is full.
    if len(_find_album_cache) >= _FIND_ALBUM_CACHE_MAX:
        try:
            _find_album_cache.pop(next(iter(_find_album_cache)))
        except StopIteration:
            pass

    _find_album_cache[cache_key] = result
    return result


def get_album_count():
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute('SELECT COUNT(*) FROM albums').fetchone()[0]


def search_artists(query, limit=8):
    """Return artists matching query with their top-rated album, for autocomplete."""
    norm = _normalize(query)
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            '''
            SELECT artist, album_title, year, rating
            FROM albums
            WHERE artist_norm LIKE ? OR artist LIKE ?
            ORDER BY rating DESC
            LIMIT 50
            ''',
            (f'%{norm}%', f'%{query}%')
        ).fetchall()

    seen = {}
    for artist, album, year, rating in rows:
        if artist not in seen:
            seen[artist] = {'artist': artist, 'top_album': album,
                            'top_rating': rating, 'year': year}
        elif rating > seen[artist]['top_rating']:
            seen[artist] = {'artist': artist, 'top_album': album,
                            'top_rating': rating, 'year': year}

    return list(seen.values())[:limit]


def search_albums(query, artist_filter=None, limit=8):
    """Autocomplete per album: cerca per titolo nel DB Scaruffi.
    Se artist_filter è fornito, restringe ai soli album di quell'artista.
    Se query è vuota ma artist_filter è presente, restituisce tutti gli album dell'artista."""
    norm = _normalize(query)
    with sqlite3.connect(DB_PATH) as conn:
        if artist_filter:
            anorm = _normalize(artist_filter)
            if not query:
                # Focus su album con artista già compilato → mostra tutti gli album dell'artista
                rows = conn.execute(
                    '''SELECT artist, album_title, year, rating
                       FROM albums
                       WHERE artist_norm LIKE ? OR artist LIKE ?
                       ORDER BY rating DESC LIMIT ?''',
                    (f'%{anorm}%', f'%{artist_filter}%', limit)
                ).fetchall()
            else:
                rows = conn.execute(
                    '''SELECT artist, album_title, year, rating
                       FROM albums
                       WHERE (album_norm LIKE ? OR album_title LIKE ?)
                         AND (artist_norm LIKE ? OR artist LIKE ?)
                       ORDER BY rating DESC LIMIT ?''',
                    (f'%{norm}%', f'%{query}%', f'%{anorm}%', f'%{artist_filter}%', limit * 4)
                ).fetchall()
        else:
            rows = conn.execute(
                '''SELECT artist, album_title, year, rating
                   FROM albums
                   WHERE album_norm LIKE ? OR album_title LIKE ?
                   ORDER BY rating DESC LIMIT ?''',
                (f'%{norm}%', f'%{query}%', limit * 4)
            ).fetchall()

    seen = set()
    results = []
    for artist, album, year, rating in rows:
        key = (artist.lower(), album.lower())
        if key in seen:
            continue
        seen.add(key)
        results.append({'artist': artist, 'album': album, 'year': year, 'rating': rating})

    return results[:limit]


def find_album_by_title(album_query, threshold=0.45):
    """Cerca un album solo per titolo (senza artista)."""
    from difflib import SequenceMatcher
    norm_q = _normalize(album_query)
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            'SELECT artist, album_title, year, rating FROM albums ORDER BY rating DESC'
        ).fetchall()
    best, best_score = None, 0
    for artist, album, year, rating in rows:
        score = SequenceMatcher(None, norm_q, _normalize(album)).ratio()
        if score > best_score:
            best_score = score
            best = {'artist': artist, 'album': album, 'year': year,
                    'rating': rating, 'match_score': round(score, 3)}
    if best and best_score >= threshold:
        return best
    return None


def get_mb_cache(key):
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            'SELECT value FROM mb_cache WHERE key = ?', (key,)
        ).fetchone()
    return row[0] if row else None


def set_mb_cache(key, value):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            'INSERT OR REPLACE INTO mb_cache (key, value) VALUES (?, ?)',
            (key, value)
        )
        conn.commit()


def get_top_albums(decade=None, min_rating=6.0, limit=20):
    """Top album per decade o globale, ordinati per rating desc."""
    with sqlite3.connect(DB_PATH) as conn:
        if decade:
            year_from = decade
            year_to   = decade + 9
            rows = conn.execute(
                '''SELECT artist, album_title, year, rating FROM albums
                   WHERE rating >= ? AND year >= ? AND year <= ?
                   ORDER BY rating DESC, year ASC LIMIT ?''',
                (min_rating, year_from, year_to, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                '''SELECT artist, album_title, year, rating FROM albums
                   WHERE rating >= ?
                   ORDER BY rating DESC LIMIT ?''',
                (min_rating, limit)
            ).fetchall()
    return [{'artist': r[0], 'album': r[1], 'year': r[2], 'rating': r[3]} for r in rows]


def get_stats():
    """Statistiche globali del database."""
    with sqlite3.connect(DB_PATH) as conn:
        total   = conn.execute('SELECT COUNT(*) FROM albums').fetchone()[0]
        artists = conn.execute('SELECT COUNT(DISTINCT artist) FROM albums').fetchone()[0]
        avg_r   = conn.execute('SELECT ROUND(AVG(rating),2) FROM albums').fetchone()[0]
        top10   = conn.execute('SELECT COUNT(*) FROM albums WHERE rating >= 9').fetchone()[0]
        top7    = conn.execute('SELECT COUNT(*) FROM albums WHERE rating >= 7').fetchone()[0]
        excl    = conn.execute('SELECT COUNT(*) FROM albums WHERE rating < 6').fetchone()[0]
        # Distribuzione per bucket 0-1, 1-2, ... 9-10
        buckets = {}
        for i in range(10):
            lo, hi = i, i + 1
            n = conn.execute(
                'SELECT COUNT(*) FROM albums WHERE rating >= ? AND rating < ?', (lo, hi)
            ).fetchone()[0]
            buckets[f'{lo}-{hi}'] = n
        # Top decade
        decades = conn.execute(
            '''SELECT (year/10)*10 AS decade, COUNT(*) AS cnt, ROUND(AVG(rating),2) AS avg_r
               FROM albums WHERE year IS NOT NULL AND year > 1900
               GROUP BY decade ORDER BY avg_r DESC LIMIT 5'''
        ).fetchall()
        return {
            'total': total, 'artists': artists, 'avg_rating': avg_r,
            'top_9': top10, 'top_7': top7, 'excluded': excl,
            'buckets': buckets,
            'top_decades': [{'decade': r[0], 'count': r[1], 'avg': r[2]} for r in decades]
        }


def get_random_album(min_rating=7.0):
    """Album casuale con rating >= min_rating."""
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            '''SELECT artist, album_title, year, rating FROM albums
               WHERE rating >= ? ORDER BY RANDOM() LIMIT 1''',
            (min_rating,)
        ).fetchone()
    if not row:
        return None
    return {'artist': row[0], 'album': row[1], 'year': row[2], 'rating': row[3]}


def get_similar_artists(artist, rating, limit=5):
    """Artisti con rating simile (±1.0), escludendo l'artista corrente."""
    norm = _normalize(artist)
    lo, hi = max(0, rating - 1.0), min(10, rating + 1.0)
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            '''SELECT artist, album_title, year, rating FROM albums
               WHERE rating >= ? AND rating <= ?
                 AND artist_norm != ?
               ORDER BY ABS(rating - ?) ASC, RANDOM()
               LIMIT ?''',
            (lo, hi, norm, rating, limit * 3)
        ).fetchall()
    seen = {}
    for artist_n, album, year, rat in rows:
        if artist_n not in seen:
            seen[artist_n] = {'artist': artist_n, 'album': album, 'year': year, 'rating': rat}
    return list(seen.values())[:limit]


def get_hall_of_fame(min_rating=9.0, limit=50):
    """Album con rating >= 9: la Hall of Fame di Scaruffi."""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            '''SELECT artist, album_title, year, rating FROM albums
               WHERE rating >= ? ORDER BY rating DESC, year ASC LIMIT ?''',
            (min_rating, limit)
        ).fetchall()
    return [{'artist': r[0], 'album': r[1], 'year': r[2], 'rating': r[3]} for r in rows]


def get_decade_champions():
    """Il miglior album (rating massimo) per ogni decade."""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            '''SELECT (year/10)*10 AS decade, artist, album_title, year, MAX(rating) AS rating
               FROM albums
               WHERE year IS NOT NULL AND year > 1900
               GROUP BY decade
               ORDER BY decade ASC'''
        ).fetchall()
    return [{'decade': r[0], 'artist': r[1], 'album': r[2], 'year': r[3], 'rating': r[4]}
            for r in rows]
