"""
MusicBrainz API wrapper.

Used to:
  1. Find which album a recording belongs to.
  2. Check whether a recording was released as a standalone single.
  3. Rank singles for a given artist/album by number of MB releases
     (more releases ≈ more prominent single).

Rate limit: 1 request per second (enforced here).
"""

import requests
import time
import json
import re
from difflib import SequenceMatcher
import database as db

_MB_BASE = "https://musicbrainz.org/ws/2"
_HEADERS = {
    "User-Agent": "ScaruffiChecker/1.0 (personal-research)",
    "Accept": "application/json",
}
_last_call = 0.0


def _get(endpoint, params):
    global _last_call
    elapsed = time.time() - _last_call
    if elapsed < 1.1:
        time.sleep(1.1 - elapsed)
    params["fmt"] = "json"
    try:
        r = requests.get(f"{_MB_BASE}/{endpoint}", params=params,
                         headers=_HEADERS, timeout=12)
        r.raise_for_status()
        _last_call = time.time()
        return r.json()
    except Exception as e:
        print(f"[MB] error: {e}")
        _last_call = time.time()
        return None


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def find_recording(title, artist=None):
    """
    Search MusicBrainz for a recording.

    Double-check logic:
      1. Prefer ORIGINAL studio albums (exclude Compilation, Live, Remix,
         Soundtrack, Demo, DJ-mix secondary types).
      2. Among valid albums, take the EARLIEST release date — this is the
         original album the song first appeared on, not a reissue/anthology.
      3. Fall back to EP → Single only if no studio album is found.

    Returns a list of dicts:
      { title, artist, mbid, album, album_id, album_type, year }
    Returns [] if nothing found.
    """
    cache_key = f"rec2:{title.lower()}|{(artist or '').lower()}"
    cached = db.get_mb_cache(cache_key)
    if cached:
        return json.loads(cached)

    q = f'recording:"{_esc(title)}"'
    if artist:
        q += f' AND artist:"{_esc(artist)}"'

    data = _get("recording", {"query": q, "limit": 15,
                               "inc": "releases+artists+release-groups"})
    if not data:
        return []

    # Release types excluded from "original album" consideration
    _EXCLUDED_SECONDARY = {
        "Compilation", "Live", "Remix", "DJ-mix",
        "Mixtape/Street", "Demo", "Soundtrack", "Interview", "Spokenword",
    }

    results = []
    seen = set()   # (artist_lower, album_lower)

    for rec in data.get("recordings", []):
        rec_title = rec.get("title", "")
        credits = rec.get("artist-credit", [])
        rec_artist = credits[0]["artist"]["name"] if credits else ""

        # ── Collect & score every release for this recording ────────────────
        studio_candidates = []
        fallback_candidates = []

        for rel in rec.get("releases", []):
            rg = rel.get("release-group", {})
            primary_type = rg.get("primary-type", "")
            secondary_types = {
                s if isinstance(s, str) else s.get("name", "")
                for s in rg.get("secondary-types", [])
            }

            album_title = rel.get("title", "")
            album_id    = rel.get("id", "")
            # Prefer release-group's first-release-date (original year, not reissue)
            rg_date  = rg.get("first-release-date") or ""
            rel_date = rel.get("date") or ""
            date     = rg_date or rel_date
            year     = _parse_year(date)
            sort_key = date or "9999-99-99"

            if primary_type == "Album" and not (secondary_types & _EXCLUDED_SECONDARY):
                # Original studio album — best choice
                studio_candidates.append({
                    "album": album_title, "album_id": album_id,
                    "album_type": "Album", "year": year, "_sort": sort_key,
                })
            elif primary_type in ("EP", "Single"):
                fallback_candidates.append({
                    "album": album_title, "album_id": album_id,
                    "album_type": primary_type, "year": year, "_sort": sort_key,
                })

        # Use the EARLIEST studio album; fall back to EP > Single if none
        for pool in (studio_candidates, fallback_candidates):
            pool.sort(key=lambda x: x["_sort"])

        chosen = (studio_candidates or fallback_candidates)
        if not chosen:
            continue

        best = chosen[0]
        key = (rec_artist.lower(), best["album"].lower())
        if key in seen:
            continue
        seen.add(key)

        results.append({
            "title":      rec_title,
            "artist":     rec_artist,
            "mbid":       rec.get("id", ""),
            "album":      best["album"],
            "album_id":   best["album_id"],
            "album_type": best["album_type"],
            "year":       best["year"],
        })

    # Studio albums first, then EP, then singles
    _type_order = {"Album": 0, "EP": 1, "Single": 2}
    results.sort(key=lambda x: _type_order.get(x["album_type"], 9))

    db.set_mb_cache(cache_key, json.dumps(results))
    return results


def is_track_a_single(recording_title, artist_name):
    """
    Returns (is_single: bool, singles: list).
    is_single is True if the track was released as a standalone single.
    singles is the list of single release-groups found.
    """
    cache_key = f"single:{recording_title.lower()}|{artist_name.lower()}"
    cached = db.get_mb_cache(cache_key)
    if cached:
        d = json.loads(cached)
        return d["is_single"], d["singles"]

    q = f'recording:"{_esc(recording_title)}" AND artist:"{_esc(artist_name)}"'
    data = _get("recording", {"query": q, "limit": 5,
                               "inc": "releases+release-groups"})

    singles = []
    if data:
        for rec in data.get("recordings", []):
            for rel in rec.get("releases", []):
                rg = rel.get("release-group", {})
                if rg.get("primary-type") == "Single":
                    singles.append({
                        "title": rel.get("title", ""),
                        "date": rel.get("date", ""),
                        "id": rel.get("id", ""),
                    })

    # Deduplicate by title
    seen_titles = set()
    deduped = []
    for s in singles:
        t = s["title"].lower()
        if t not in seen_titles:
            seen_titles.add(t)
            deduped.append(s)

    is_single = len(deduped) > 0
    db.set_mb_cache(cache_key, json.dumps({"is_single": is_single, "singles": deduped}))
    return is_single, deduped


def get_top_singles_for_artist(artist_name, around_year=None, limit=50):
    """
    Return singles by artist from MusicBrainz, sorted by number of releases
    (proxy for prominence).  Optionally filter to ±3 years of around_year.

    Returns list of { title, date, release_count }.
    """
    cache_key = f"topsingles:{artist_name.lower()}|{around_year}"
    cached = db.get_mb_cache(cache_key)
    if cached:
        return json.loads(cached)

    q = f'artist:"{_esc(artist_name)}" AND type:single'
    data = _get("release-group", {"query": q, "limit": limit})

    if not data:
        return []

    singles = []
    for rg in data.get("release-groups", []):
        if rg.get("primary-type") != "Single":
            continue
        title = rg.get("title", "")
        date = rg.get("first-release-date", "")
        year = _parse_year(date)

        # Strict year filter: ±2 years from album year to avoid other artists
        # with the same name (e.g. Belgian "Placebo" vs UK "Placebo")
        if around_year and year and abs(year - around_year) > 2:
            continue

        score = int(rg.get("score", 0))
        singles.append({
            "title": title,
            "date":  date,
            "year":  year,
            "id":    rg.get("id", ""),
            "score": score,
        })

    # Rank by MusicBrainz relevance score desc (proxy for prominence/popularity)
    singles.sort(key=lambda x: x.get("score", 0), reverse=True)

    db.set_mb_cache(cache_key, json.dumps(singles))
    return singles


def get_top_2_singles(artist_name, album_year=None):
    """
    Return the top 2 most prominent singles for the artist
    (used for the >6 rule).
    """
    return get_top_singles_for_artist(artist_name, around_year=album_year)[:2]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _esc(s):
    """Escape special Lucene characters for MusicBrainz query."""
    return re.sub(r'([+\-&|!(){}\[\]^"~*?:\\])', r'\\\1', s)


def _parse_year(date_str):
    if not date_str:
        return None
    m = re.match(r'^(\d{4})', date_str)
    return int(m.group(1)) if m else None
