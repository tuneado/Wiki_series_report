# -*- coding: utf-8 -*-
"""
Supabase Cache Module — Persistent caching for Wiki Series Report.

Caches Fandom page data, IMDb lookups, and TMDb data in Supabase PostgreSQL
to speed up repeat queries and share cache across all users.
"""

import os
import json
from typing import Optional
from datetime import datetime
from dotenv import load_dotenv
import streamlit as st

# --- Supabase Setup ---
load_dotenv("vars.env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Fallback to Streamlit secrets if env vars not set
if not SUPABASE_URL:
    try:
        SUPABASE_URL = st.secrets["SUPABASE_URL"]
    except (KeyError, FileNotFoundError):
        pass
if not SUPABASE_KEY:
    try:
        SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
    except (KeyError, FileNotFoundError):
        pass

_supabase_client = None


def _get_client():
    """Get or create a Supabase client (lazy initialization)."""
    global _supabase_client
    if _supabase_client is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("[CACHE] ❌ Supabase not configured - caching disabled")
            print(f"[CACHE]    SUPABASE_URL: {'SET' if SUPABASE_URL else 'MISSING'}")
            print(f"[CACHE]    SUPABASE_KEY: {'SET' if SUPABASE_KEY else 'MISSING'}")
            return None
        
        # Validate key format (should be a JWT starting with eyJ)
        if not SUPABASE_KEY.startswith("eyJ"):
            print(f"[CACHE] ⚠️ SUPABASE_KEY appears invalid (should start with 'eyJ...')")
            print(f"[CACHE]    Current key starts with: {SUPABASE_KEY[:20]}...")
            print(f"[CACHE]    Get the 'anon public' key from Supabase → Settings → API")
        
        try:
            from supabase import create_client
            _supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
            print(f"[CACHE] ✅ Supabase client initialized: {SUPABASE_URL}")
        except Exception as e:
            print(f"[CACHE] ❌ Failed to initialize Supabase: {e}")
            return None
    return _supabase_client


# =============================================================================
# Fandom Cache
# =============================================================================

def get_fandom_cache(url: str) -> Optional[dict]:
    """Get cached Fandom page data.
    
    Returns:
        Dict with titulo_original, direcao_atores, direcao_tecnica, or None if not cached.
    """
    client = _get_client()
    if not client:
        return None
    try:
        result = client.table("fandom_cache").select("*").eq("url", url).execute()
        if result.data:
            row = result.data[0]
            print(f"[CACHE] HIT fandom_cache: {url[:50]}...")
            return {
                "Título Original": row.get("titulo_original"),
                "Direção de Atores": row.get("direcao_atores"),
                "Direção Técnica": row.get("direcao_tecnica"),
            }
    except Exception as e:
        print(f"[CACHE] Error reading fandom_cache: {e}")
    return None


def save_fandom_cache(url: str, titulo_original: str, direcao_atores: str, direcao_tecnica: str) -> bool:
    """Save Fandom page data to cache.
    
    Returns:
        True if saved successfully, False otherwise.
    """
    client = _get_client()
    if not client:
        return False
    try:
        data = {
            "url": url,
            "titulo_original": titulo_original,
            "direcao_atores": direcao_atores,
            "direcao_tecnica": direcao_tecnica,
        }
        # Upsert: insert or update if exists
        client.table("fandom_cache").upsert(data, on_conflict="url").execute()
        print(f"[CACHE] SAVED fandom_cache: {url[:50]}...")
        return True
    except Exception as e:
        print(f"[CACHE] Error saving fandom_cache: {e}")
        return False


# =============================================================================
# IMDb Cache
# =============================================================================

def get_imdb_cache(search_title: str) -> Optional[dict]:
    """Get cached IMDb lookup result.
    
    Returns:
        Dict with imdb_id, matched_title, kind, confidence_level, confidence_score, or None.
    """
    client = _get_client()
    if not client:
        return None
    try:
        # Normalize search title for consistent lookups
        normalized = search_title.lower().strip()
        result = client.table("imdb_cache").select("*").eq("search_title", normalized).execute()
        if result.data:
            row = result.data[0]
            print(f"[CACHE] HIT imdb_cache: {search_title}")
            return {
                "imdb_id": row.get("imdb_id"),
                "matched_title": row.get("matched_title"),
                "kind": row.get("kind"),
                "confidence_level": row.get("confidence_level"),
                "confidence_score": row.get("confidence_score"),
            }
    except Exception as e:
        print(f"[CACHE] Error reading imdb_cache: {e}")
    return None


def save_imdb_cache(
    search_title: str,
    imdb_id: str,
    matched_title: str,
    kind: str,
    confidence_level: str,
    confidence_score: float,
) -> bool:
    """Save IMDb lookup result to cache.
    
    Returns:
        True if saved successfully, False otherwise.
    """
    client = _get_client()
    if not client:
        return False
    try:
        normalized = search_title.lower().strip()
        data = {
            "search_title": normalized,
            "imdb_id": imdb_id,
            "matched_title": matched_title,
            "kind": kind,
            "confidence_level": confidence_level,
            "confidence_score": confidence_score,
        }
        client.table("imdb_cache").upsert(data, on_conflict="search_title").execute()
        print(f"[CACHE] SAVED imdb_cache: {search_title} → {imdb_id}")
        return True
    except Exception as e:
        print(f"[CACHE] Error saving imdb_cache: {e}")
        return False


# =============================================================================
# TMDb Shows Cache
# =============================================================================

def get_tmdb_show_cache(imdb_id: str) -> Optional[dict]:
    """Get cached TMDb TV show data.
    
    Returns:
        Dict with tmdb_id, original_name, seasons (list), or None if not cached.
    """
    client = _get_client()
    if not client:
        return None
    try:
        result = client.table("tmdb_shows").select("*").eq("imdb_id", imdb_id).execute()
        if result.data:
            row = result.data[0]
            print(f"[CACHE] HIT tmdb_shows: {imdb_id}")
            seasons_json = row.get("seasons_json")
            # JSONB returns Python list directly, but handle string fallback
            if isinstance(seasons_json, str):
                seasons = json.loads(seasons_json)
            else:
                seasons = seasons_json or []
            return {
                "tmdb_id": row.get("tmdb_id"),
                "original_name": row.get("original_name"),
                "seasons": seasons,
            }
    except Exception as e:
        print(f"[CACHE] Error reading tmdb_shows: {e}")
    return None


def save_tmdb_show_cache(
    imdb_id: str,
    tmdb_id: int,
    original_name: str,
    seasons: list,
) -> bool:
    """Save TMDb TV show data to cache.
    
    Args:
        imdb_id: IMDb ID (without 'tt' prefix).
        tmdb_id: TMDb show ID.
        original_name: Original show title.
        seasons: List of season dicts with keys: season_number, air_date, episode_count.
    
    Returns:
        True if saved successfully, False otherwise.
    """
    client = _get_client()
    if not client:
        return False
    try:
        data = {
            "imdb_id": imdb_id,
            "tmdb_id": tmdb_id,
            "original_name": original_name,
            "seasons_json": seasons,  # JSONB accepts Python list directly
        }
        client.table("tmdb_shows").upsert(data, on_conflict="imdb_id").execute()
        print(f"[CACHE] SAVED tmdb_shows: {imdb_id} ({original_name})")
        return True
    except Exception as e:
        print(f"[CACHE] Error saving tmdb_shows: {e}")
        return False


# =============================================================================
# TMDb Movies Cache
# =============================================================================

def get_tmdb_movie_cache(imdb_id: str) -> Optional[dict]:
    """Get cached TMDb movie data.
    
    Returns:
        Dict with tmdb_id, original_title, release_year, or None if not cached.
    """
    client = _get_client()
    if not client:
        return None
    try:
        result = client.table("tmdb_movies").select("*").eq("imdb_id", imdb_id).execute()
        if result.data:
            row = result.data[0]
            print(f"[CACHE] HIT tmdb_movies: {imdb_id}")
            return {
                "tmdb_id": row.get("tmdb_id"),
                "original_title": row.get("original_title"),
                "release_year": row.get("release_year"),
            }
    except Exception as e:
        print(f"[CACHE] Error reading tmdb_movies: {e}")
    return None


def save_tmdb_movie_cache(
    imdb_id: str,
    tmdb_id: int,
    original_title: str,
    release_year: str,
) -> bool:
    """Save TMDb movie data to cache.
    
    Returns:
        True if saved successfully, False otherwise.
    """
    client = _get_client()
    if not client:
        return False
    try:
        data = {
            "imdb_id": imdb_id,
            "tmdb_id": tmdb_id,
            "original_title": original_title,
            "release_year": release_year,
        }
        client.table("tmdb_movies").upsert(data, on_conflict="imdb_id").execute()
        print(f"[CACHE] SAVED tmdb_movies: {imdb_id} ({original_title})")
        return True
    except Exception as e:
        print(f"[CACHE] Error saving tmdb_movies: {e}")
        return False


# =============================================================================
# Cache Management
# =============================================================================

def get_cache_stats() -> dict:
    """Get cache statistics.
    
    Returns:
        Dict with entry counts for each table.
    """
    client = _get_client()
    stats = {
        "fandom_cache": 0,
        "imdb_cache": 0,
        "tmdb_shows": 0,
        "tmdb_movies": 0,
        "total": 0,
        "connected": client is not None,
    }
    if not client:
        return stats
    
    try:
        for table in ["fandom_cache", "imdb_cache", "tmdb_shows", "tmdb_movies"]:
            result = client.table(table).select("*", count="exact").execute()
            stats[table] = result.count or 0
        stats["total"] = sum(stats[t] for t in ["fandom_cache", "imdb_cache", "tmdb_shows", "tmdb_movies"])
    except Exception as e:
        print(f"[CACHE] Error getting stats: {e}")
    
    return stats


def clear_cache(table: Optional[str] = None) -> bool:
    """Clear cache entries.
    
    Args:
        table: Specific table to clear, or None to clear all tables.
    
    Returns:
        True if cleared successfully, False otherwise.
    """
    client = _get_client()
    if not client:
        return False
    
    tables = [table] if table else ["fandom_cache", "imdb_cache", "tmdb_shows", "tmdb_movies"]
    
    try:
        for t in tables:
            # Delete all rows (Supabase requires a filter, so we use a condition that matches all)
            client.table(t).delete().neq("created_at", "1900-01-01").execute()
            print(f"[CACHE] CLEARED table: {t}")
        return True
    except Exception as e:
        print(f"[CACHE] Error clearing cache: {e}")
        return False


def search_cache(query: str, limit: int = 50) -> list[dict]:
    """Search cache entries by title/URL.
    
    Args:
        query: Search query string.
        limit: Maximum results to return.
    
    Returns:
        List of matching cache entries.
    """
    client = _get_client()
    if not client:
        return []
    
    results = []
    query_lower = f"%{query.lower()}%"
    
    try:
        # Search fandom_cache
        fandom = client.table("fandom_cache").select("*").ilike("url", query_lower).limit(limit).execute()
        for row in fandom.data or []:
            results.append({
                "type": "fandom",
                "key": row["url"],
                "title": row.get("titulo_original", "N/A"),
                "created_at": row.get("created_at"),
            })
        
        # Search imdb_cache
        imdb = client.table("imdb_cache").select("*").ilike("search_title", query_lower).limit(limit).execute()
        for row in imdb.data or []:
            results.append({
                "type": "imdb",
                "key": row["search_title"],
                "title": row.get("matched_title", "N/A"),
                "imdb_id": row.get("imdb_id"),
                "created_at": row.get("created_at"),
            })
        
        # Search tmdb_shows
        shows = client.table("tmdb_shows").select("*").ilike("original_name", query_lower).limit(limit).execute()
        for row in shows.data or []:
            results.append({
                "type": "tmdb_show",
                "key": row["imdb_id"],
                "title": row.get("original_name", "N/A"),
                "tmdb_id": row.get("tmdb_id"),
                "created_at": row.get("created_at"),
            })
        
        # Search tmdb_movies
        movies = client.table("tmdb_movies").select("*").ilike("original_title", query_lower).limit(limit).execute()
        for row in movies.data or []:
            results.append({
                "type": "tmdb_movie",
                "key": row["imdb_id"],
                "title": row.get("original_title", "N/A"),
                "tmdb_id": row.get("tmdb_id"),
                "created_at": row.get("created_at"),
            })
    except Exception as e:
        print(f"[CACHE] Error searching cache: {e}")
    
    return results


def delete_cache_entry(table: str, key: str) -> bool:
    """Delete a specific cache entry.
    
    Args:
        table: Table name ('fandom_cache', 'imdb_cache', 'tmdb_shows', 'tmdb_movies').
        key: Primary key value (url, search_title, or imdb_id).
    
    Returns:
        True if deleted, False otherwise.
    """
    client = _get_client()
    if not client:
        return False
    
    key_column = {
        "fandom_cache": "url",
        "imdb_cache": "search_title",
        "tmdb_shows": "imdb_id",
        "tmdb_movies": "imdb_id",
    }.get(table)
    
    if not key_column:
        return False
    
    try:
        client.table(table).delete().eq(key_column, key).execute()
        print(f"[CACHE] DELETED {table}: {key}")
        return True
    except Exception as e:
        print(f"[CACHE] Error deleting entry: {e}")
        return False


def get_recent_entries(table: str, limit: int = 10) -> list[dict]:
    """Get recent entries from a cache table.
    
    Args:
        table: Table name ('fandom_cache', 'imdb_cache', 'tmdb_shows', 'tmdb_movies').
        limit: Maximum entries to return.
    
    Returns:
        List of row dicts, newest first.
    """
    client = _get_client()
    if not client:
        return []
    
    try:
        result = client.table(table).select("*").order("created_at", desc=True).limit(limit).execute()
        return result.data or []
    except Exception as e:
        print(f"[CACHE] Error getting recent entries from {table}: {e}")
        return []


def test_connection() -> tuple[bool, str]:
    """Test Supabase connection.
    
    Returns:
        Tuple of (success, message).
    """
    client = _get_client()
    if not client:
        return False, "Cliente Supabase não inicializado. Verifique SUPABASE_URL e SUPABASE_KEY."
    
    try:
        result = client.table("fandom_cache").select("url").limit(1).execute()
        return True, "Ligação bem sucedida!"
    except Exception as e:
        error_str = str(e)
        if "relation" in error_str and "does not exist" in error_str:
            return False, "Tabelas não existem. Execute o SQL de criação no Supabase."
        elif "Invalid API key" in error_str:
            return False, "Chave API inválida."
        else:
            return False, f"Erro: {error_str}"