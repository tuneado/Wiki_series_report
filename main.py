# -*- coding: utf-8 -*-
"""
Wiki Series Report — Core scraping & data enrichment logic.

Scrapes Portuguese voice acting data from Wikidobragens Fandom wiki,
enriches with season/year data from IMDb (Cinemagoer) and TMDb (tmdbsimple).
"""

import os
import re
import time
import requests
import pandas as pd
import requests_cache
from bs4 import BeautifulSoup
from collections import defaultdict
from typing import Optional
from imdb import Cinemagoer
import tmdbsimple as tmdb
import streamlit as st
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, Browser, Page, BrowserContext

# Import cache module for Supabase persistence
import cache as db_cache

# Enable requests caching for TMDb API calls (7-day cache)
requests_cache.install_cache(
    'tmdb_cache',
    backend='sqlite',
    expire_after=604800,  # 7 days in seconds
    allowable_methods=['GET'],
)

# --- Setup ---
load_dotenv("vars.env")
API_KEY = os.getenv("TMDB_API_KEY")
if not API_KEY:
    try:
        API_KEY = st.secrets["TMDB_API_KEY"]
    except (KeyError, FileNotFoundError):
        API_KEY = None
    if not API_KEY:
        st.error("❌ No TMDB API KEY defined in vars.env or Streamlit secrets.")
        st.stop()

tmdb.API_KEY = API_KEY

# Fix SSL certificate issues on macOS by using certifi's certificate bundle
import ssl
import certifi
os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()

BASE_URL: str = "https://wikidobragens.fandom.com"
TEST_MODE: bool = False
TEST_MODE_LIMIT: int = 16

error_logs: dict[str, str] = {}  # Maps show title to error messages

# Global Playwright browser instance and context (reused across requests)
_playwright_instance = None
_browser_instance: Optional[Browser] = None
_browser_context: Optional[BrowserContext] = None


def _get_browser_context() -> Optional[BrowserContext]:
    """Get or create a reusable Playwright browser context."""
    global _browser_context
    if _browser_context is None:
        browser = _get_browser()
        if browser is None:
            return None
        _browser_context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="pt-PT",
        )
        _log("Browser context created (will be reused)")
    return _browser_context


_playwright_browsers_installed = False
_playwright_available = True  # Set to False if Playwright fails to initialize

def _ensure_playwright_browsers():
    """Install Playwright browsers if not already installed."""
    global _playwright_browsers_installed
    if not _playwright_browsers_installed:
        import subprocess
        try:
            _log("Installing Playwright browsers (first run)...")
            subprocess.run(["playwright", "install", "chromium"], check=True, capture_output=True, timeout=120)
            _playwright_browsers_installed = True
            _log("Playwright browsers installed successfully")
        except Exception as e:
            _log(f"Warning: Could not install Playwright browsers: {e}")
            _playwright_browsers_installed = True  # Don't retry

def _get_browser() -> Optional[Browser]:
    """Get or create a Playwright browser instance (lazy initialization)."""
    global _playwright_instance, _browser_instance, _playwright_available
    if not _playwright_available:
        return None
    if _browser_instance is None:
        _ensure_playwright_browsers()
        _log("Starting Playwright browser...")
        try:
            _playwright_instance = sync_playwright().start()
            _browser_instance = _playwright_instance.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ]
            )
            _log("Browser started successfully")
        except Exception as e:
            _log(f"Playwright failed to start: {e} - using requests only")
            _playwright_available = False
            return None
    return _browser_instance


def _close_browser() -> None:
    """Close the Playwright browser instance and context."""
    global _playwright_instance, _browser_instance, _browser_context
    if _browser_context:
        _log("Closing browser context...")
        try:
            _browser_context.close()
        except:
            pass
        _browser_context = None
    if _browser_instance:
        _log("Closing Playwright browser...")
        try:
            _browser_instance.close()
        except:
            pass
        _browser_instance = None
    if _playwright_instance:
        try:
            _playwright_instance.stop()
        except:
            pass
        _playwright_instance = None


def _fetch_with_requests(url: str, log_callback=None) -> Optional[bytes]:
    """Try to fetch URL with simple requests (faster, no JS).
    
    Returns:
        Response content bytes, or None if failed.
    """
    def ui_log(msg):
        _log(msg)
        if log_callback:
            log_callback(msg)
    
    try:
        ui_log(f"[REQUESTS] Starting fetch: {url.split('/')[-1]}")
        # Full browser-like headers to avoid 403 bot detection
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "pt-PT,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "Cache-Control": "max-age=0",
        }
        # Use a session to handle cookies properly
        session = requests.Session()
        response = session.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        content = response.content
        ui_log(f"[REQUESTS] Got response: {len(content)} bytes")
        # Check if we got substantial content (not a Cloudflare challenge)
        if len(content) > 10000 and b"Just a moment" not in content:
            ui_log(f"[REQUESTS] Success!")
            return content
        ui_log(f"[REQUESTS] Blocked by Cloudflare, trying Playwright...")
        return None
    except requests.exceptions.Timeout:
        ui_log(f"[REQUESTS] Timeout after 20s")
        return None
    except Exception as e:
        ui_log(f"[REQUESTS] Failed: {type(e).__name__}: {e}")
        return None


def _fetch_with_retry(url: str, max_retries: int = 3, log_callback=None) -> Optional[bytes]:
    """Fetch URL using Playwright (handles Cloudflare JS challenges).
    
    Args:
        url: URL to fetch.
        max_retries: Maximum number of retry attempts.
        log_callback: Optional callback for real-time logging.
    
    Returns:
        Response content bytes, or None if all retries failed.
    """
    global _browser_context, _playwright_available
    
    def ui_log(msg):
        _log(msg)
        if log_callback:
            log_callback(msg)
    
    # Try simple requests first (faster, works on Streamlit Cloud)
    result = _fetch_with_requests(url, log_callback)
    if result:
        return result
    
    # Skip Playwright if not available
    if not _playwright_available:
        ui_log(f"Playwright not available, cannot fetch {url}")
        return None
    
    # Fall back to Playwright for JS-rendered pages
    ui_log(f"Trying Playwright for {url}...")
    
    for attempt in range(max_retries):
        page = None
        try:
            ui_log(f"[Playwright] Attempt {attempt + 1}/{max_retries}")
            context = _get_browser_context()
            if context is None:
                ui_log("[Playwright] Browser unavailable, giving up")
                return None
            page = context.new_page()
            
            # Use 'domcontentloaded' for faster initial load, then wait for content
            ui_log("[Playwright] Loading page...")
            response = page.goto(url, wait_until="domcontentloaded", timeout=20000)
            
            # Wait for the page body to have substantial content (Cloudflare challenge passed)
            try:
                page.wait_for_function(
                    "document.body && document.body.innerText.length > 1000",
                    timeout=15000
                )
            except:
                # If wait fails, check if we have enough content anyway
                pass
            
            content = page.content()
            content_bytes = content.encode("utf-8")
            ui_log(f"[Playwright] Got {len(content_bytes)} bytes")
            
            # Check if we got a real page (not a challenge page)
            if len(content_bytes) > 10000:
                page.close()
                ui_log("[Playwright] Success!")
                return content_bytes
            
            # Small response might be a challenge page, retry with fresh context
            ui_log(f"[Playwright] Small response, retrying...")
            page.close()
            
            # Reset context on failure (might be stale)
            if attempt > 0:
                ui_log("[Playwright] Resetting browser...")
                try:
                    _browser_context.close()
                except:
                    pass
                _browser_context = None
            
            time.sleep(2 * (attempt + 1))
            continue
            
        except Exception as e:
            ui_log(f"[Playwright] Error: {type(e).__name__}")
            if page:
                try:
                    page.close()
                except:
                    pass
            
            # Reset context on timeout errors
            if "timeout" in str(e).lower():
                ui_log("[Playwright] Timeout, resetting...")
                try:
                    if _browser_context:
                        _browser_context.close()
                except:
                    pass
                _browser_context = None
            
            time.sleep(2 * (attempt + 1))
    
    ui_log(f"[Playwright] All attempts failed")
    return None


def _log(msg: str) -> None:
    """Print a debug message to the console (visible in the terminal running Streamlit)."""
    print(f"[DEBUG] {msg}")


def _calculate_confidence(search_title: str, result_title: str) -> tuple[str, float]:
    """Calculate match confidence between search query and result.
    
    Returns:
        Tuple of (confidence_level, score) where level is 'exact'|'high'|'medium'|'low'
    """
    from difflib import SequenceMatcher
    
    search_lower = search_title.lower().strip()
    result_lower = result_title.lower().strip()
    
    # Exact match
    if search_lower == result_lower:
        return ("exact", 1.0)
    
    # Check if one contains the other (common for subtitled shows)
    if search_lower in result_lower or result_lower in search_lower:
        return ("high", 0.9)
    
    # Use difflib for similarity ratio
    ratio = SequenceMatcher(None, search_lower, result_lower).ratio()
    
    if ratio >= 0.8:
        return ("high", ratio)
    elif ratio >= 0.6:
        return ("medium", ratio)
    return ("low", ratio)


# Reuse a single Cinemagoer instance across all calls
ia = Cinemagoer()

# --- Labels to extract from Fandom infobox ---
FANDOM_LABELS: tuple[str, ...] = ("Direção de Atores", "Direção Técnica", "Título Original")


@st.cache_data(ttl=3600, show_spinner=False)
def extract_labels_from_page(url: str, labels: tuple[str, ...]) -> dict[str, Optional[str]]:
    """Fetch metadata labels from a Fandom show/film infobox page.

    Supports both Portable Infobox (pi-data) and legacy table-based infoboxes.
    Uses Supabase cache to avoid repeated Playwright fetches.

    Args:
        url: Full URL to the Fandom page.
        labels: Tuple of label names to extract (e.g. "Direção de Atores").

    Returns:
        Dict mapping each label to its extracted value, or None if not found.
    """
    # Handle None or empty URL
    if not url:
        return {label: None for label in labels}

    # Check Supabase cache first
    cached = db_cache.get_fandom_cache(url)
    if cached:
        result = {label: None for label in labels}
        result["Título Original"] = cached.get("Título Original")
        result["Direção de Atores"] = cached.get("Direção de Atores")
        result["Direção Técnica"] = cached.get("Direção Técnica")
        print(f"   [CACHE] Loaded from Supabase: {url.rsplit('/', 1)[-1]}")
        return result

    try:
        content = _fetch_with_retry(url, max_retries=2)
        if not content:
            raise ValueError("Failed to fetch page after retries")
        soup = BeautifulSoup(content, "html.parser")

        result: dict[str, Optional[str]] = {label: None for label in labels}

        # Helper to normalize label text for matching (handle whitespace, special chars)
        def normalize_label(text: str) -> str:
            return text.strip().replace("\u00a0", " ").replace("\u200b", "")

        # Create a lookup dict for normalized labels
        label_lookup = {normalize_label(label): label for label in labels}

        # Helper to extract first value when multiple are present (separated by <br> or newlines)
        def extract_first_value(tag) -> str:
            """Extract just the first value from a tag that may contain multiple values."""
            # Check for <br> tags - content before first <br> is usually the primary value
            br_tag = tag.find("br")
            if br_tag:
                # Get text before the first <br>
                first_text = ""
                for content in tag.children:
                    if content.name == "br":
                        break
                    if hasattr(content, 'get_text'):
                        first_text += content.get_text(strip=True)
                    elif isinstance(content, str):
                        first_text += content.strip()
                if first_text.strip():
                    return first_text.strip()
            
            # Check for multiple paragraphs or divs
            first_p = tag.find("p")
            if first_p:
                return first_p.get_text(strip=True)
            
            # Fallback: get all text but split by common separators
            full_text = tag.get_text(" ", strip=True)
            # Split by patterns that indicate multiple values
            for sep in [" / ", " | ", " - ", "\n"]:
                if sep in full_text:
                    return full_text.split(sep)[0].strip()
            
            return full_text

        # Method 1: Portable Infobox (div.pi-data with h3.pi-data-label)
        data_blocks = soup.find_all("div", class_="pi-data")
        for block in data_blocks:
            label_tag = block.find("h3", class_="pi-data-label")
            value_tag = block.find("div", class_="pi-data-value")

            if not label_tag or not value_tag:
                continue

            label_text = normalize_label(label_tag.get_text(strip=True))
            if label_text in label_lookup:
                # For Título Original, extract only the first value (in case of renamed shows)
                if label_text == "Título Original":
                    result[label_lookup[label_text]] = extract_first_value(value_tag)
                else:
                    result[label_lookup[label_text]] = value_tag.get_text(" ", strip=True)

        # Method 2: Legacy table-based infobox (table.infobox with th/td pairs)
        if not any(result.values()):
            infobox = soup.find("table", class_="infobox")
            if infobox:
                for row in infobox.find_all("tr"):
                    th = row.find("th")
                    td = row.find("td")
                    if th and td:
                        label_text = normalize_label(th.get_text(strip=True))
                        if label_text in label_lookup:
                            result[label_lookup[label_text]] = td.get_text(" ", strip=True)

        # Method 3: Data-source attributes (some portable infoboxes use data-source)
        if not any(result.values()):
            for label in labels:
                # Try data-source attribute with various possible values
                normalized = label.lower().replace(" ", "_").replace("í", "i")
                elem = soup.find(attrs={"data-source": normalized})
                if elem:
                    value_div = elem.find("div", class_="pi-data-value")
                    if value_div:
                        result[label] = value_div.get_text(" ", strip=True)

        # Method 4: Search all h3 elements within portable-infobox for label matches
        if not result.get("Título Original"):
            infobox = soup.find("aside", class_="portable-infobox")
            if infobox:
                for h3 in infobox.find_all("h3"):
                    label_text = normalize_label(h3.get_text(strip=True))
                    if label_text in label_lookup:
                        # Find the associated value (next sibling div or parent's value div)
                        parent = h3.parent
                        value_div = parent.find("div", class_="pi-data-value") if parent else None
                        if value_div:
                            result[label_lookup[label_text]] = value_div.get_text(" ", strip=True)

        # Method 5: Search for pi-item elements (alternative portable infobox structure)
        if not result.get("Título Original"):
            for item in soup.find_all("div", class_="pi-item"):
                label_tag = item.find("h3")
                value_tag = item.find("div", class_="pi-data-value")
                if label_tag and value_tag:
                    label_text = normalize_label(label_tag.get_text(strip=True))
                    if label_text in label_lookup:
                        result[label_lookup[label_text]] = value_tag.get_text(" ", strip=True)

        # Method 6: Search for data-source="titulo_original" or similar
        if not result.get("Título Original"):
            for ds_val in ["titulo_original", "titulo-original", "titulooriginal", "original"]:
                elem = soup.find(attrs={"data-source": ds_val})
                if elem:
                    value_div = elem.find("div", class_="pi-data-value")
                    if value_div:
                        result["Título Original"] = value_div.get_text(" ", strip=True)
                        break

        # Method 7: Find h3 with "Título Original" text and get sibling/parent value
        if not result.get("Título Original"):
            for h3 in soup.find_all("h3"):
                h3_text = normalize_label(h3.get_text(strip=True))
                if "Título Original" in h3_text or h3_text == "Título Original":
                    # Try to find value in parent's pi-data-value div
                    parent = h3.find_parent("div", class_="pi-data")
                    if parent:
                        value_div = parent.find("div", class_="pi-data-value")
                        if value_div:
                            result["Título Original"] = value_div.get_text(" ", strip=True)
                            break
                    # Also try next sibling
                    next_sib = h3.find_next_sibling("div", class_="pi-data-value")
                    if next_sib:
                        result["Título Original"] = next_sib.get_text(" ", strip=True)
                        break

        # Method 8: Look for h3 label and get next sibling text (common Fandom structure)
        # Structure: <h3 class="pi-data-label">Título Original</h3> followed by value in next element
        if not result.get("Título Original"):
            for h3 in soup.find_all("h3", class_="pi-data-label"):
                h3_text = normalize_label(h3.get_text(strip=True))
                if h3_text == "Título Original":
                    # Get next sibling that contains the value
                    for sibling in h3.next_siblings:
                        if hasattr(sibling, 'get_text'):
                            val = sibling.get_text(strip=True)
                            if val:
                                result["Título Original"] = val
                                break
                        elif isinstance(sibling, str) and sibling.strip():
                            result["Título Original"] = sibling.strip()
                            break
                    if result.get("Título Original"):
                        break

        # Method 9: Search within section divs (pi-smart-data-value)
        if not result.get("Título Original"):
            for elem in soup.find_all("div", class_="pi-smart-data-value"):
                # Check if previous sibling or parent has Título Original label
                prev = elem.find_previous_sibling()
                if prev and "Título Original" in prev.get_text():
                    result["Título Original"] = elem.get_text(strip=True)
                    break

        # Debug: Print all h3 labels found to help diagnose
        if not result.get("Título Original"):
            all_h3_labels = [h3.get_text(strip=True) for h3 in soup.find_all("h3")]
            print(f"   DEBUG: All h3 texts in page: {all_h3_labels[:20]}")  # First 20

        # Log what was found
        found = {k: v for k, v in result.items() if v}
        if found:
            print(f"   ✓ Extracted from {url.rsplit('/', 1)[-1]}: {found}")
            # Save to Supabase cache
            db_cache.save_fandom_cache(
                url=url,
                titulo_original=result.get("Título Original") or "",
                direcao_atores=result.get("Direção de Atores") or "",
                direcao_tecnica=result.get("Direção Técnica") or "",
            )
        else:
            print(f"   ⚠ No infobox labels found for {url.rsplit('/', 1)[-1]}")

        return result

    except Exception as e:
        msg = f"❌ Error accessing {url}: {e}"
        print(msg)
        page_name = url.rsplit("/", 1)[-1].replace("_", " ") if "/" in url else url
        error_logs[page_name] = msg
        return {label: None for label in labels}


def _find_tmdb_show_by_imdb_id(imdb_id: str) -> Optional[dict]:
    """Find a TMDb TV show using its IMDb ID for accurate matching.

    Args:
        imdb_id: The IMDb ID (numeric string, without 'tt' prefix).

    Returns:
        TMDb show details dict, or None if not found.
    """
    try:
        find = tmdb.Find(f"tt{imdb_id}")
        find.info(external_source="imdb_id")
        tv_results = find.tv_results
        if not tv_results:
            return None
        show_id = tv_results[0]["id"]
        tv_show = tmdb.TV(show_id)
        return tv_show.info()
    except Exception:
        return None


def _find_tmdb_movie_by_imdb_id(imdb_id: str) -> Optional[dict]:
    """Find a TMDb movie using its IMDb ID for accurate matching.

    Args:
        imdb_id: The IMDb ID (numeric string, without 'tt' prefix).

    Returns:
        TMDb movie details dict, or None if not found.
    """
    try:
        find = tmdb.Find(f"tt{imdb_id}")
        find.info(external_source="imdb_id")
        movie_results = find.movie_results
        if not movie_results:
            return None
        movie_id = movie_results[0]["id"]
        movie = tmdb.Movies(movie_id)
        return movie.info()
    except Exception:
        return None


# --- Expand Seasons using IMDb and TMDb ---
def get_seasons_as_rows(
    title: str,
    base_row: pd.Series,
    status=None,
) -> list[dict]:
    """Expand a single show into one row per season with year/episode data.

    Args:
        title: Show name as displayed on the wiki.
        base_row: Original DataFrame row for this show.
        status: Streamlit status placeholder for progress messages.

    Returns:
        List of row dicts (one per season), or empty list on failure.
    """
    fandom_url = base_row["URL"]
    extra = extract_labels_from_page(fandom_url, FANDOM_LABELS)
    titulo_original = extra.get("Título Original")

    # Log extraction results
    print(f"   → Wiki Title: '{title}'")
    print(f"   → Título Original extracted: '{titulo_original}'")
    print(f"   → All extracted labels: {extra}")

    base_row = base_row.to_dict()
    base_row.update({
        "Direção de Atores": extra.get("Direção de Atores", ""),
        "Direção Técnica": extra.get("Direção Técnica", ""),
        "Título Original": titulo_original or title,
    })

    # --- IMDb lookup: try multiple search strategies ---
    search_titles = []
    if titulo_original:
        search_titles.append(titulo_original)
    if title and title != titulo_original:
        search_titles.append(title)
    
    # Ensure we always have at least one search term
    if not search_titles:
        search_titles = [title] if title else []
    
    import re
    
    # Build a comprehensive list of search variations
    all_search_titles = list(search_titles)  # Start with original titles
    
    for search_title in search_titles:
        # Add cleaned title (remove parenthetical notes)
        clean_title = re.sub(r'\s*\([^)]*\)\s*', ' ', search_title).strip()
        if clean_title != search_title and clean_title not in all_search_titles:
            all_search_titles.append(clean_title)
        
        # Add main title before colon/dash
        main_title = re.split(r'[:\-–—]', search_title)[0].strip()
        if main_title and main_title != search_title and len(main_title) > 3 and main_title not in all_search_titles:
            all_search_titles.append(main_title)
        
        # Remove common non-English patterns that IMDb won't recognize
        patterns_to_remove = [
            r'\s*:\s*Les Aventures de\s*',      # French
            r'\s*:\s*As Aventuras de\s*',       # Portuguese
            r'\s*:\s*Las Aventuras de\s*',      # Spanish
            r'\s*:\s*Tales of\s*',              # English subtitle
            r'\s*:\s*Contos de\s*',             # Portuguese
            r'\s+et\s+',                         # French "and"
            r'\s+e\s+',                          # Portuguese "and" (with spaces)
        ]
        simplified = search_title
        for pattern in patterns_to_remove:
            simplified = re.sub(pattern, ' ', simplified, flags=re.IGNORECASE)
        simplified = re.sub(r'\s+', ' ', simplified).strip()
        if simplified != search_title and simplified not in all_search_titles and len(simplified) > 3:
            all_search_titles.append(simplified)
    
    print(f"   → All search variations: {all_search_titles}")

    # --- Check IMDb cache first ---
    show = None
    cached_imdb = None
    for search_title in all_search_titles:
        cached_imdb = db_cache.get_imdb_cache(search_title)
        if cached_imdb and cached_imdb.get("imdb_id"):
            print(f"   [CACHE] IMDb cache hit for: {search_title}")
            break
    
    # If we have a cached IMDb result, check TMDb cache too
    if cached_imdb and cached_imdb.get("imdb_id"):
        imdb_id = cached_imdb["imdb_id"]
        cached_tmdb = db_cache.get_tmdb_show_cache(imdb_id)
        if cached_tmdb and cached_tmdb.get("seasons"):
            print(f"   [CACHE] TMDb cache hit for IMDb ID: {imdb_id}")
            original_title = cached_tmdb["original_name"]
            confidence_level = cached_imdb.get("confidence_level", "medium")
            confidence_score = cached_imdb.get("confidence_score", 0.7)
            
            rows = []
            for season in cached_tmdb["seasons"]:
                season_number = season.get("season_number")
                year = season.get("air_date", "N/A")[:4] if season.get("air_date") else "N/A"
                
                row = dict(base_row)
                row.update({
                    "Título Original": original_title,
                    "Temporada": "Especiais" if season_number == 0 else season_number,
                    "Ano Lançamento": year,
                    "Total Episódios": int(season.get("episode_count", 0)),
                    "Match Confidence": confidence_level,
                    "Match Score": round(confidence_score, 2) if confidence_score else 0.0,
                })
                rows.append(row)
            return rows

    # --- Live IMDb search ---
    for search_title in all_search_titles:
        msg = f"🔍 Searching IMDb for: {search_title}"
        print(msg)
        if status:
            status.write(msg)
        results = ia.search_movie(search_title)
        
        if not results:
            print(f"   → No IMDb results at all for '{search_title}'")
            continue

        # Check first 15 results for TV shows (skip ia.update if kind already available)
        tv_kinds = {"tv series", "tv mini series", "tv miniseries", "tv movie", "tv short"}
        kinds_found = []
        for result in results[:15]:
            result_kind = result.get("kind")
            # Only call ia.update() if kind is not already available
            if not result_kind:
                try:
                    ia.update(result)
                    result_kind = result.get("kind", "unknown")
                except Exception:
                    result_kind = "unknown"
            kinds_found.append(f"{result.get('title', '?')} ({result_kind})")
            
            # Select first TV-related result
            if not show and (result_kind in tv_kinds or "tv" in result_kind.lower() or "series" in result_kind.lower()):
                show = result
                print(f"   → Selected: {result.get('title')} (kind: {result_kind})")

        print(f"   → IMDb results: {kinds_found}")

        if show:
            break

    # --- Fallback: TMDb direct search (better international title support) ---
    if not show:
        print(f"   → IMDb failed, trying TMDb search...")
        for search_title in all_search_titles:
            try:
                search = tmdb.Search()
                search.tv(query=search_title)
                if search.results:
                    tmdb_result = search.results[0]
                    tmdb_id = tmdb_result["id"]
                    tv_show = tmdb.TV(tmdb_id)
                    tmdb_show = tv_show.info()
                    
                    original_title = tmdb_show.get("original_name") or tmdb_show.get("name") or titulo_original or title
                    msg = f"🎬 Found via TMDb: {original_title} (TMDb ID: {tmdb_id})"
                    print(msg)
                    if status:
                        status.write(msg)
                    
                    # Build rows directly from TMDb data
                    confidence_level, confidence_score = _calculate_confidence(search_title, original_title)
                    rows = []
                    for season in tmdb_show.get("seasons", []):
                        season_number = season.get("season_number")
                        air_date = season.get("air_date")
                        year = air_date[:4] if air_date else "N/A"
                        
                        row = dict(base_row)
                        row.update({
                            "Título Original": original_title,
                            "Temporada": "Especiais" if season_number == 0 else season_number,
                            "Ano Lançamento": year,
                            "Total Episódios": int(season.get("episode_count", 0)),
                            "Match Confidence": confidence_level,
                            "Match Score": round(confidence_score, 2),
                        })
                        rows.append(row)
                    
                    # Save to cache (use tmdb_id as key since no IMDb ID)
                    tmdb_key = f"tmdb_{tmdb_id}"
                    db_cache.save_imdb_cache(
                        search_title=titulo_original or title,
                        imdb_id=tmdb_key,
                        matched_title=original_title,
                        kind="tv series",
                        confidence_level=confidence_level,
                        confidence_score=confidence_score,
                    )
                    seasons_for_cache = [
                        {
                            "season_number": s.get("season_number"),
                            "air_date": s.get("air_date"),
                            "episode_count": s.get("episode_count", 0),
                        }
                        for s in tmdb_show.get("seasons", [])
                    ]
                    db_cache.save_tmdb_show_cache(
                        imdb_id=tmdb_key,
                        tmdb_id=tmdb_id,
                        original_name=original_title,
                        seasons=seasons_for_cache,
                    )
                    return rows
            except Exception as e:
                print(f"   → TMDb search error for '{search_title}': {e}")
                continue
        
        # Both IMDb and TMDb failed
        msg = f"❌ No TV series found on IMDb or TMDb for: {', '.join(search_titles)}"
        print(msg)
        if status:
            status.error(msg)
        error_logs[title] = msg
        return []

    ia.update(show)
    original_title = show.get("title") or titulo_original or title
    msg = f"🎬 Found: {original_title} ({show.movieID}) — Type: {show.get('kind')}"
    print(msg)
    if status:
        status.write(msg)

    # Calculate confidence based on what we searched vs what we found
    confidence_level, confidence_score = _calculate_confidence(titulo_original or title, original_title)

    # --- TMDb lookup by IMDb ID (accurate match) ---
    try:
        tmdb_show = _find_tmdb_show_by_imdb_id(show.movieID)
        if not tmdb_show:
            raise ValueError(f"TMDb show not found via IMDb ID tt{show.movieID}")
    except Exception as e:
        msg = f"❌ TMDb error: {e}"
        print(msg)
        if status:
            status.error(msg)
        error_logs[title] = msg
        return []

    # Save to caches for future lookups
    db_cache.save_imdb_cache(
        search_title=titulo_original or title,
        imdb_id=show.movieID,
        matched_title=original_title,
        kind=show.get("kind", "tv series"),
        confidence_level=confidence_level,
        confidence_score=confidence_score,
    )
    
    # Prepare seasons data for cache
    seasons_for_cache = [
        {
            "season_number": s.get("season_number"),
            "air_date": s.get("air_date"),
            "episode_count": s.get("episode_count", 0),
        }
        for s in tmdb_show.get("seasons", [])
    ]
    db_cache.save_tmdb_show_cache(
        imdb_id=show.movieID,
        tmdb_id=tmdb_show.get("id"),
        original_name=tmdb_show.get("original_name") or original_title,
        seasons=seasons_for_cache,
    )

    rows = []
    for season in tmdb_show.get("seasons", []):
        season_number = season.get("season_number")
        air_date = season.get("air_date")
        year = air_date[:4] if air_date else "N/A"

        row = dict(base_row)
        row.update({
            "Título Original": original_title,  # Use IMDb's actual title
            "Temporada": "Especiais" if season_number == 0 else season_number,
            "Ano Lançamento": year,
            "Total Episódios": int(season.get("episode_count", 0)),
            "Match Confidence": confidence_level,
            "Match Score": round(confidence_score, 2),
        })
        rows.append(row)
    return rows


def get_film_row(
    title: str,
    base_row: pd.Series,
    status=None,
) -> list[dict]:
    """Get metadata for a single film from IMDb/TMDb.

    Args:
        title: Film name as displayed on the wiki.
        base_row: Original DataFrame row for this film.
        status: Streamlit status placeholder for progress messages.

    Returns:
        List with a single row dict, or empty list on failure.
    """
    fandom_url = base_row["URL"]
    extra = extract_labels_from_page(fandom_url, FANDOM_LABELS)
    titulo_original = extra.get("Título Original")

    base_row = base_row.to_dict()
    base_row.update({
        "Direção de Atores": extra.get("Direção de Atores", ""),
        "Direção Técnica": extra.get("Direção Técnica", ""),
        "Título Original": titulo_original or title,
    })

    # --- IMDb lookup: try multiple search strategies ---
    search_titles = []
    if titulo_original:
        search_titles.append(titulo_original)
    if title and title != titulo_original:
        search_titles.append(title)
    
    # Add title variations for better matching
    for base_title in [titulo_original, title]:
        if not base_title:
            continue
        # Clean title: remove parenthetical info
        clean = re.sub(r"\s*\([^)]*\)\s*", " ", base_title).strip()
        if clean and clean not in search_titles:
            search_titles.append(clean)
        # Main title before colon
        if ":" in base_title:
            main_part = base_title.split(":")[0].strip()
            if main_part and main_part not in search_titles:
                search_titles.append(main_part)

    # --- Check IMDb cache first ---
    cached_imdb = None
    for search_title in search_titles:
        cached_imdb = db_cache.get_imdb_cache(search_title)
        if cached_imdb and cached_imdb.get("imdb_id"):
            print(f"   [CACHE] IMDb cache hit for film: {search_title}")
            break
    
    # If we have a cached IMDb result, check TMDb cache too
    if cached_imdb and cached_imdb.get("imdb_id"):
        imdb_id = cached_imdb["imdb_id"]
        cached_tmdb = db_cache.get_tmdb_movie_cache(imdb_id)
        if cached_tmdb:
            print(f"   [CACHE] TMDb movie cache hit for IMDb ID: {imdb_id}")
            row = dict(base_row)
            row.update({
                "Título Original": cached_tmdb["original_title"],
                "Temporada": "N/A",
                "Ano Lançamento": cached_tmdb.get("release_year", "N/A"),
                "Total Episódios": "N/A",
                "Tipo": "Filme",
                "Match Confidence": cached_imdb.get("confidence_level", "medium"),
                "Match Score": round(cached_imdb.get("confidence_score", 0.7), 2),
            })
            return [row]

    # --- Live IMDb search ---
    movie = None
    matched_search_title = None
    for search_title in search_titles:
        msg = f"🔍 Searching IMDb for film: {search_title}"
        print(msg)
        if status:
            status.write(msg)
        results = ia.search_movie(search_title)
        if not results:
            continue

        # Check more results (up to 15)
        for result in results[:15]:
            result_kind = result.get("kind")
            if result_kind in ("movie", "video movie", "tv movie"):
                movie = result
                matched_search_title = search_title
                break

        if movie:
            break

    # --- TMDb fallback search for films ---
    if not movie:
        msg = f"⚠️ IMDb não encontrou filme, tentando TMDb..."
        print(msg)
        if status:
            status.write(msg)
        
        for search_title in search_titles:
            msg = f"🔍 Searching TMDb for film: {search_title}"
            print(msg)
            if status:
                status.write(msg)
            try:
                search = tmdb.Search()
                response = search.movie(query=search_title)
                if search.results:
                    tmdb_result = search.results[0]
                    tmdb_movie_id = tmdb_result["id"]
                    tmdb_movie = tmdb.Movies(tmdb_movie_id).info()
                    
                    original_title = tmdb_movie.get("original_title") or tmdb_movie.get("title") or titulo_original or title
                    release_date = tmdb_movie.get("release_date", "")
                    year = release_date[:4] if release_date else "N/A"
                    
                    confidence_level, confidence_score = _calculate_confidence(titulo_original or title, original_title)
                    
                    row = dict(base_row)
                    row.update({
                        "Título Original": original_title,
                        "Temporada": "N/A",
                        "Ano Lançamento": year,
                        "Total Episódios": "N/A",
                        "Tipo": "Filme",
                        "Match Confidence": confidence_level,
                        "Match Score": round(confidence_score, 2),
                    })
                    msg = f"🎬 Found film via TMDb: {original_title} (TMDb ID: {tmdb_movie_id})"
                    print(msg)
                    if status:
                        status.write(msg)
                    
                    # Save to caches (use TMDb ID as key since no IMDb ID)
                    tmdb_key = f"tmdb_{tmdb_movie_id}"
                    db_cache.save_imdb_cache(
                        search_title=titulo_original or title,
                        imdb_id=tmdb_key,
                        matched_title=original_title,
                        kind="movie",
                        confidence_level=confidence_level,
                        confidence_score=confidence_score,
                    )
                    db_cache.save_tmdb_movie_cache(
                        imdb_id=tmdb_key,
                        tmdb_id=tmdb_movie_id,
                        original_title=original_title,
                        release_year=year,
                    )
                    return [row]
            except Exception as e:
                _log(f"TMDb film search error: {e}")
                continue
        
        msg = f"❌ No movie found on IMDb/TMDb for: {', '.join(search_titles)}"
        print(msg)
        if status:
            status.error(msg)
        error_logs[title] = msg
        return []

    ia.update(movie)
    original_title = movie.get("title") or titulo_original or title
    msg = f"🎬 Found film: {original_title} ({movie.movieID}) — Type: {movie.get('kind')}"
    print(msg)
    if status:
        status.write(msg)

    # --- TMDb lookup by IMDb ID ---
    try:
        tmdb_movie = _find_tmdb_movie_by_imdb_id(movie.movieID)
        if not tmdb_movie:
            raise ValueError(f"TMDb movie not found via IMDb ID tt{movie.movieID}")
    except Exception as e:
        msg = f"❌ TMDb error (film): {e}"
        print(msg)
        if status:
            status.error(msg)
        error_logs[title] = msg
        return []

    release_date = tmdb_movie.get("release_date", "")
    year = release_date[:4] if release_date else "N/A"
    
    # Calculate confidence
    confidence_level, confidence_score = _calculate_confidence(titulo_original or title, original_title)

    # Save to caches for future lookups
    db_cache.save_imdb_cache(
        search_title=titulo_original or title,
        imdb_id=movie.movieID,
        matched_title=original_title,
        kind=movie.get("kind", "movie"),
        confidence_level=confidence_level,
        confidence_score=confidence_score,
    )
    db_cache.save_tmdb_movie_cache(
        imdb_id=movie.movieID,
        tmdb_id=tmdb_movie.get("id"),
        original_title=tmdb_movie.get("original_title") or original_title,
        release_year=year,
    )

    row = dict(base_row)
    row.update({
        "Título Original": original_title,
        "Temporada": "N/A",
        "Ano Lançamento": year,
        "Total Episódios": "N/A",
        "Tipo": "Filme",
        "Match Confidence": confidence_level,
        "Match Score": round(confidence_score, 2),
    })
    return [row]


DEFAULT_COLUMNS: list[str] = ["Nome", "Personagem", "Estúdio"]


def _parse_wiki_table(
    soup: BeautifulSoup,
    section_name: str,
    max_items: Optional[int] = None,
) -> tuple[pd.DataFrame, defaultdict]:
    """Parse a wiki table under a given section heading.

    Handles pages where table headers contain only images (no text) by
    falling back to DEFAULT_COLUMNS.  Accepts any <table> (not just
    those with the ``article-table`` class) and stops at the next heading.

    Args:
        soup: Parsed BeautifulSoup of the full wiki page.
        section_name: The mw-headline text to find (e.g. "Séries", "Filmes").
        max_items: Optional limit on number of items to parse.

    Returns:
        Tuple of (DataFrame with parsed data, char_map defaultdict(set)).
    """
    char_map: defaultdict[str, set] = defaultdict(set)
    studio_map: defaultdict[str, set] = defaultdict(set)

    _log(f"--- _parse_wiki_table('{section_name}') ---")

    # --- Locate the section heading (try multiple methods) ---
    section = None
    heading_parent = None
    
    # Method 1: Classic mw-headline spans
    all_headlines = soup.find_all("span", class_="mw-headline")
    _log(f"Found {len(all_headlines)} mw-headline spans on page")
    for span in all_headlines:
        raw = span.get_text(strip=True)
        text = raw.replace("\u200b", "").replace("\u00a0", " ").strip()
        parent_tag = span.parent.name if span.parent else "?"
        _log(f"  headline: '{text}' (raw repr: {repr(raw)}) parent=<{parent_tag}>")
        if text == section_name:
            section = span
            heading_parent = span.parent
            _log(f"  >>> MATCHED '{section_name}' via mw-headline")
            break

    # Method 2: Direct h2/h3 elements (Fandom UCP style)
    if not section:
        _log("Trying h2/h3 direct search...")
        for tag in soup.find_all(["h2", "h3"]):
            raw = tag.get_text(strip=True)
            text = raw.replace("\u200b", "").replace("\u00a0", " ").replace("[edit]", "").replace("[editar]", "").strip()
            _log(f"  h-tag: <{tag.name}> '{text}'")
            if section_name in text or text == section_name:
                section = tag
                heading_parent = tag
                _log(f"  >>> MATCHED '{section_name}' via direct h-tag")
                break
    
    # Method 3: Section headers with data-section attribute
    if not section:
        _log("Trying data-section search...")
        for tag in soup.find_all(attrs={"data-section": True}):
            raw = tag.get_text(strip=True)
            text = raw.replace("\u200b", "").replace("\u00a0", " ").strip()
            _log(f"  data-section: '{text}'")
            if section_name in text:
                section = tag
                heading_parent = tag.parent if tag.parent else tag
                _log(f"  >>> MATCHED '{section_name}' via data-section")
                break

    # Debug: show first few h2/h3 elements if nothing found
    if not section:
        _log("DEBUG: First 10 h2/h3 elements on page:")
        for i, tag in enumerate(soup.find_all(["h2", "h3"])[:10]):
            _log(f"  {i}: <{tag.name}> {repr(tag.get_text(strip=True)[:50])}")
        _log(f"Section '{section_name}' NOT FOUND — returning empty")
        return pd.DataFrame(), char_map

    # --- Walk siblings to find the first <table> before the next heading ---
    table = None
    heading_tag = heading_parent if heading_parent else section.parent  # e.g. <h2> or <h3>
    _log(f"Heading parent tag: <{heading_tag.name if heading_tag else 'None'}>")
    current = heading_tag
    sibling_idx = 0
    while current:
        current = current.find_next_sibling()
        if not current:
            _log("  sibling walk ended (no more siblings)")
            break
        sibling_idx += 1
        tag_name = current.name or "[text]"
        classes = current.get("class", []) if current.name else []
        _log(f"  sibling #{sibling_idx}: <{tag_name}> class={classes}")
        # Stop at the next heading of equal or higher level
        if current.name in ("h2", "h3"):
            _log(f"  stopped at next heading <{current.name}>")
            break
        if current.name == "table":
            table = current
            _log(f"  >>> Found <table> class={classes}")
            break
        # Also look for a table nested inside a wrapper div
        nested = current.find("table") if current.name == "div" else None
        if nested:
            table = nested
            _log("  >>> Found nested <table> inside <div>")
            break

    # Fallback: search for the first table after the heading anywhere
    if not table and heading_tag:
        table = heading_tag.find_next("table")
        if table:
            _log("  >>> Fallback: found <table> via find_next()")
        else:
            _log("  >>> Fallback: NO table found anywhere after heading")
    
    # Last resort: search for table after the section element itself
    if not table and section:
        table = section.find_next("table")
        if table:
            _log("  >>> Last resort: found <table> via section.find_next()")

    if not table:
        _log(f"No table found for '{section_name}' — returning empty")
        return pd.DataFrame(), char_map

    # --- Determine column headers ---
    all_rows = table.find_all("tr")
    _log(f"Table has {len(all_rows)} <tr> rows")
    if not all_rows:
        _log("Table has 0 rows — returning empty")
        return pd.DataFrame(), char_map

    raw_headers = [th.get_text(strip=True) for th in all_rows[0].find_all(["th", "td"])]
    _log(f"Row 0 raw headers ({len(raw_headers)} cells): {raw_headers}")

    # If headers are blank (image-only), fall back to defaults
    non_empty = [h for h in raw_headers if h]
    if non_empty:
        headers = raw_headers
        _log(f"Using detected headers: {headers}")
    else:
        headers = list(DEFAULT_COLUMNS)
        _log(f"All headers empty — falling back to DEFAULT_COLUMNS: {headers}")
        # Pad/trim to match actual column count
        ncols_first_data = 0
        for r in all_rows[1:]:
            tds = r.find_all("td")
            if tds:
                ncols_first_data = len(tds)
                break
        if ncols_first_data > len(headers):
            headers += [f"Col{i}" for i in range(len(headers), ncols_first_data)]
        elif ncols_first_data and ncols_first_data < len(headers):
            headers = headers[:ncols_first_data]

    name_idx = headers.index("Nome") if "Nome" in headers else 0
    char_idx = headers.index("Personagem") if "Personagem" in headers else 1
    studio_idx = headers.index("Estúdio") if "Estúdio" in headers else None
    _log(f"Final headers ({len(headers)}): {headers}  name_idx={name_idx} char_idx={char_idx} studio_idx={studio_idx}")

    # --- Parse data rows (handle rowspan for merged studio cells) ---
    data: list[list[str]] = []
    links: list[Optional[str]] = []
    current_name: Optional[str] = None
    current_link: Optional[str] = None

    # Track rowspan: maps column index -> (value, remaining_rows)
    rowspan_tracker: dict[int, tuple[str, int]] = {}

    for i, row in enumerate(all_rows[1:]):
        if TEST_MODE and i >= TEST_MODE_LIMIT:
            break
        if max_items and len(data) >= max_items:
            break

        cols = row.find_all("td")
        # Skip header-only rows (rows that only have <th> cells)
        if not cols:
            continue

        # Build values array, accounting for rowspan from previous rows
        values: list[str] = []
        col_iter = iter(cols)
        for col_idx in range(len(headers)):
            # Check if this column has an active rowspan from previous rows
            if col_idx in rowspan_tracker:
                val, remaining = rowspan_tracker[col_idx]
                values.append(val)
                if remaining > 1:
                    rowspan_tracker[col_idx] = (val, remaining - 1)
                else:
                    del rowspan_tracker[col_idx]
            else:
                # Get next actual cell
                cell = next(col_iter, None)
                if cell:
                    cell_text = cell.get_text(strip=True)
                    values.append(cell_text)
                    # Check for rowspan attribute
                    rowspan = cell.get("rowspan")
                    if rowspan and int(rowspan) > 1:
                        rowspan_tracker[col_idx] = (cell_text, int(rowspan) - 1)
                else:
                    values.append("")

        # Skip completely empty rows
        if not any(values):
            continue

        # Detect studio-only continuation rows:
        # - Row has fewer actual cells than expected (1-2 cells when we expect 3+)
        # - Character column would be empty
        # - Previous show exists (current_name is set)
        # These rows list additional studios for the previous show
        expected_cols = len(headers)
        actual_cols = len(cols)
        char_val_raw = values[char_idx] if char_idx < len(values) else ""
        is_studio_continuation = (
            actual_cols < expected_cols and
            not char_val_raw and
            current_name is not None and
            actual_cols >= 1
        )

        if is_studio_continuation:
            # First cell is likely a studio name (may have a link)
            potential_studio = cols[0].get_text(strip=True) if cols else ""
            if potential_studio:
                studio_map[current_name].add(potential_studio)
                _log(f"  Studio continuation: added '{potential_studio}' to '{current_name}'")
            continue  # Don't treat this as a new show

        link_tag = cols[name_idx].find("a", href=True) if name_idx < len(cols) else None
        cell_text = cols[name_idx].get_text(strip=True) if name_idx < len(cols) else ""

        # Extract studio value for this row (from values which accounts for rowspan)
        studio_val = values[studio_idx] if studio_idx is not None and studio_idx < len(values) else ""

        if link_tag:
            current_name = cell_text
            href = link_tag["href"]
            current_link = href if href.startswith("http") else BASE_URL + href
            char = values[char_idx] if char_idx < len(values) else ""
            if char:
                char_map[current_name].add(char)
            if studio_val:
                studio_map[current_name].add(studio_val)
        else:
            char = values[char_idx] if char_idx < len(values) else ""
            if char or cell_text:
                if current_name:
                    char_map[current_name].add(char or cell_text)
            if studio_val and current_name:
                studio_map[current_name].add(studio_val)

        if link_tag or char:
            data.append(values[:len(headers)])
            links.append(current_link if link_tag else None)

    _log(f"Parsed {len(data)} data rows, {len(links)} links")

    if not data:
        _log("No data rows parsed — returning empty")
        return pd.DataFrame(), char_map

    df = pd.DataFrame(data, columns=headers)
    df["Nome"] = df["Nome"].replace("", pd.NA).ffill()
    df["URL"] = links

    # Helper to sort studios by season number (extract from patterns like "T1", "T1-T3", "(T4-T6)")
    def sort_studios_by_season(studios: set) -> str:
        import re
        def extract_first_season(s: str) -> int:
            # Match patterns like T1, T1-T3, (T1-T3), T.1, etc.
            match = re.search(r'[Tt]\.?(\d+)', s)
            if match:
                return int(match.group(1))
            return 999  # Studios without season info go last
        
        sorted_studios = sorted(studios, key=extract_first_season)
        return " / ".join(sorted_studios) if sorted_studios else "N/A"

    # Combine all studios per show, sorted by season number
    if "Estúdio" in df.columns:
        df["Estúdio"] = df["Nome"].map(
            lambda nm: sort_studios_by_season(studio_map.get(nm, set()))
        )

    df["Personagens (Todos)"] = df["Nome"].map(
        lambda nm: ", ".join(sorted(char_map.get(nm, set())))
    )

    # Filter and clean
    if "Personagem" in df.columns:
        df = df[~(df["URL"].isna() & df["Personagem"].isna() & ~df["Nome"].isin(char_map))]
        df = df.drop("Personagem", axis=1)

    _log(f"Returning DataFrame with {len(df)} rows, cols={list(df.columns)}")
    return df, char_map


def _expand_rows(
    df: pd.DataFrame,
    expand_fn,
    status=None,
    progress_bar=None,
    item_label: str = "show",
    progress_offset: float = 0.0,
    progress_scale: float = 1.0,
    start_item: int = 1,
    log_callback=None,
) -> list[dict]:
    """Expand DataFrame rows by calling expand_fn for each unique show/film.

    Deduplicates by title so each show is only processed once.

    Args:
        df: DataFrame with at least "Nome" and "URL" columns.
        expand_fn: Callable(title, row, status) -> list[dict].
        status: Streamlit status placeholder.
        progress_bar: Streamlit progress bar widget.
        item_label: Label for status messages ("série" or "filme").
        progress_offset: Starting offset for progress bar (0.0 to 1.0).
        progress_scale: Scale factor for this batch within the overall progress.
        start_item: 1-based index of the first unique item to process (skip earlier ones).
        log_callback: Optional callback function for real-time logging.

    Returns:
        List of expanded row dicts.
    """
    def ui_log(msg):
        _log(msg)
        if log_callback:
            log_callback(msg)
    
    expanded: list[dict] = []
    seen_titles: set[str] = set()
    total = df["Nome"].nunique()
    processed = 0
    unique_index = 0  # 1-based counter of unique titles encountered

    for _, row in df.iterrows():
        title = row["Nome"]

        # Deduplicate: skip if already processed
        if title in seen_titles:
            continue
        seen_titles.add(title)
        unique_index += 1

        # Skip items before the requested start
        if unique_index < start_item:
            continue

        processed += 1
        msg = f"[{unique_index}/{total}] {item_label}: {title}"
        ui_log(msg)
        if status:
            status.write(f"📺 {msg}")
        if progress_bar is not None:
            pct = progress_offset + (processed / total if total > 0 else 1.0) * progress_scale
            progress_bar.progress(min(pct, 1.0))

        # Extract Título Original from Fandom page (cached, so no duplicate requests)
        fandom_url = row.get("URL")
        if fandom_url:
            ui_log(f"  Fetching Fandom page: {fandom_url.split('/')[-1]}")
            extra = extract_labels_from_page(fandom_url, FANDOM_LABELS)
            titulo_original = extra.get("Título Original") or title
        else:
            titulo_original = title

        ui_log(f"  Searching IMDb/TMDb for: {titulo_original}")
        result_rows = expand_fn(title, row, status)
        err = error_logs.get(title, "")

        if result_rows:
            ui_log(f"  Found {len(result_rows)} season(s)")
            for s in result_rows:
                s["Error Log"] = err
                if not s.get("Título Original"):
                    s["Título Original"] = titulo_original
                # Sanitize values
                for k, v in s.items():
                    if isinstance(v, (dict, list)):
                        s[k] = str(v)
                expanded.append(s)
        else:
            ui_log(f"  No results, using fallback")
            fallback = row.to_dict()
            # Preserve extracted Fandom labels in fallback
            fallback.update({
                "Título Original": titulo_original,
                "Direção de Atores": extra.get("Direção de Atores", "") if fandom_url else "",
                "Direção Técnica": extra.get("Direção Técnica", "") if fandom_url else "",
                "Temporada": None,
                "Ano Lançamento": None,
                "Total Episódios": None,
                "Error Log": err,
                "Match Confidence": "none",
                "Match Score": 0.0,
            })
            for k, v in fallback.items():
                if isinstance(v, (dict, list)):
                    fallback[k] = str(v)
            expanded.append(fallback)

        # Rate limiting — be polite to APIs
        time.sleep(0.3)

    return expanded


# --- Main Scraper ---
def run_scraper(
    wiki_link: str,
    status=None,
    max_items: Optional[int] = None,
    start_item: int = 1,
    include_series: bool = True,
    include_films: bool = False,
    progress_bar=None,
    log_callback=None,
) -> pd.DataFrame:
    """Scrape an artist's wiki page and enrich with IMDb/TMDb data.

    Args:
        wiki_link: Full URL to the Wikidobragens artist page.
        status: Streamlit status placeholder for progress messages.
        max_items: Optional limit on number of items to process.
        start_item: 1-based index of the first unique item to process.
        include_series: Whether to scrape the "Séries" section.
        include_films: Whether to scrape the "Filmes" section.
        progress_bar: Streamlit progress bar widget.
        log_callback: Optional callback function for real-time logging.

    Returns:
        DataFrame with one row per show-season (and optionally one row per film).
    """
    def ui_log(msg):
        _log(msg)
        if log_callback:
            log_callback(msg)
    
    # Clear stale errors from previous runs
    error_logs.clear()

    ui_log(f"Fetching: {wiki_link}")
    content = _fetch_with_retry(wiki_link, max_retries=3, log_callback=log_callback)
    if not content:
        _close_browser()  # Cleanup on error
        if status:
            status.error("❌ Falha ao aceder à página da wiki (Cloudflare blocking). Tente novamente mais tarde.")
        return pd.DataFrame()
    
    ui_log(f"Page fetched, size: {len(content)} bytes")
    soup = BeautifulSoup(content, "html.parser")
    if status:
        status.write("🔍 A procurar tabela na Fandom...")

    # --- Parse Séries (optional) ---
    series_df = pd.DataFrame()
    has_series = False
    if include_series:
        if status:
            status.write("🔍 A procurar tabela de Séries na Fandom...")
        ui_log("Parsing Séries table...")
        series_df, _ = _parse_wiki_table(soup, "Séries", max_items)
        has_series = not series_df.empty
        ui_log(f"Séries result: {'FOUND' if has_series else 'EMPTY'} ({len(series_df)} rows)")
        if has_series:
            series_df["Error Log"] = ""
            series_df["Tipo"] = "Série"

    # --- Parse Filmes (optional) ---
    films_df = pd.DataFrame()
    has_films = False
    if include_films:
        if status:
            status.write("🔍 A procurar tabela de Filmes na Fandom...")
        ui_log("Parsing Filmes table...")
        films_df, _ = _parse_wiki_table(soup, "Filmes", max_items)
        has_films = not films_df.empty
        ui_log(f"Filmes result: {'FOUND' if has_films else 'EMPTY'} ({len(films_df)} rows)")
        if has_films:
            films_df["Error Log"] = ""
            films_df["Tipo"] = "Filme"

    if not has_series and not has_films:
        ui_log("ERROR: No tables found on page")
        if status:
            status.error("❌ Nenhuma tabela encontrada.")
        return pd.DataFrame()

    # Compute progress bar proportions
    if has_series and has_films:
        series_weight = 0.7
        films_weight = 0.3
    elif has_series:
        series_weight = 1.0
        films_weight = 0.0
    else:  # only films
        series_weight = 0.0
        films_weight = 1.0

    # --- Expand Séries ---
    expanded: list[dict] = []
    if has_series:
        if status:
            status.write("📺 A processar séries...")
        ui_log(f"Processing {len(series_df)} series...")
        expanded.extend(
            _expand_rows(
                series_df, get_seasons_as_rows, status, progress_bar,
                item_label="série", progress_offset=0.0, progress_scale=series_weight,
                start_item=start_item, log_callback=log_callback,
            )
        )

    # --- Expand Filmes ---
    if has_films:
        if status:
            status.write("🎬 A processar filmes...")
        ui_log(f"Processing {len(films_df)} films...")
        expanded.extend(
            _expand_rows(
                films_df, get_film_row, status, progress_bar,
                item_label="filme", progress_offset=series_weight, progress_scale=films_weight,
                start_item=start_item, log_callback=log_callback,
            )
        )

    # Cleanup Playwright browser
    _close_browser()
    ui_log(f"Done! Total rows: {len(expanded)}")
    
    return pd.DataFrame(expanded)
