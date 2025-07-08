# -*- coding: utf-8 -*-
import os
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from collections import defaultdict
from imdb import Cinemagoer
import streamlit as st
from dotenv import load_dotenv

# --- Setup ---
load_dotenv()
API_KEY = os.getenv("TMDB_API_KEY")
if not API_KEY:
    API_KEY = st.secrets.get("TMDB_API_KEY")
    if not API_KEY:
        st.error("❌ No TMDB API KEY defined in .env or Streamlit secrets.")
        st.stop()

BASE_URL = "https://wikidobragens.fandom.com"
TEST_MODE = False
error_logs = {}  # Maps show title to error messages

def extract_labels_from_page(url, labels):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        data_blocks = soup.find_all("div", class_="pi-data")

        result = {label: None for label in labels}  # Default None for all

        for block in data_blocks:
            label_tag = block.find("h3", class_="pi-data-label")
            value_tag = block.find("div", class_="pi-data-value")

            if not label_tag or not value_tag:
                continue

            label_text = label_tag.get_text(strip=True)
            if label_text in labels:
                result[label_text] = value_tag.get_text(" ", strip=True)  # Join inner text if nested

        return result

    except Exception as e:
        print(f"❌ Error accessing {url}: {e}")
        return {label: None for label in labels}

# --- Expand Seasons using IMDb and TMDb ---
def get_seasons_as_rows(title, base_row, status=None):
    fandom_url = base_row['URL']
    titulo_original = None
    if fandom_url:
        msg = f"🔍 Extraindo info da Fandom para: {title}"
        print(msg); status.write(msg)
        extra = extract_labels_from_page(fandom_url, ["Direção de Atores", "Direção Técnica", "Título Original"])
        titulo_original = extra.get("Título Original")
        if not titulo_original:
            titulo_original = title

        base_row = base_row.to_dict()
        base_row.update({
            "Direção de Atores": extra.get("Direção de Atores", ""),
            "Direção Técnica": extra.get("Direção Técnica", ""),
            "Título Original": titulo_original
        })
        search_title = titulo_original
        print(f"Título Original extracted: {extra.get('Título Original')}")

    ia = Cinemagoer()
    msg = f"🔍 Searching IMDb for: {search_title}"
    print(msg); status.write(msg)
    results = ia.search_movie(search_title)
    if not results:
        msg = "❌ No results found on IMDb."
        print(msg); status.error(msg)
        error_logs[title] = msg
        return []

    for result in results:
        ia.update(result)
        if result.get('kind') == 'tv series':
            show = result
            break
    else:
        msg = "❌ No TV series found in the results."
        print(msg); status.error(msg)
        error_logs[title] = msg
        return []

    ia.update(show)
    original_title = show.get('title') or titulo_original
    msg = f"🎬 Found: {original_title} ({show.movieID}) — Type: {show.get('kind')}"
    print(msg); status.write(msg)

    try:
        tmdb_url = "https://api.themoviedb.org/3/search/tv"
        tmdb_resp = requests.get(tmdb_url, params={"api_key": API_KEY, "query": original_title}).json()
        show_id = tmdb_resp.get("results", [{}])[0].get("id")
        if not show_id:
            raise ValueError("TMDb show not found")

        tmdb_show = requests.get(f"https://api.themoviedb.org/3/tv/{show_id}", params={"api_key": API_KEY}).json()
    except Exception as e:
        msg = f"❌ TMDb error: {e}"
        print(msg); status.error(msg)
        error_logs[title] = msg
        return []

    rows = []
    for season in tmdb_show.get("seasons", []):
        season_number = season.get("season_number")
        air_date = season.get("air_date")
        year = air_date[:4] if air_date else "N/A"

        row = dict(base_row)
        row.update({
            "Título Original": search_title,
            "Temporada": "Especiais" if season_number == 0 else season_number,
            "Ano Lançamento": year,
            "Episódios": season.get("episode_count", 0),
        })
        rows.append(row)
    return rows

# --- Main Scraper ---
def run_scraper(wiki_link, status=None, max_items=None):
    soup = BeautifulSoup(requests.get(wiki_link).content, "html.parser")
    status.write("🔍 A procurar tabela na Fandom...")

    section = soup.find("span", class_="mw-headline", string="Séries")
    table = None
    if section:
        current = section.parent
        while current:
            current = current.find_next_sibling()
            if current and current.name == "table" and "article-table" in current.get("class", []):
                table = current
                break

    data, links, char_map = [], [], defaultdict(set)
    if table:
        rows = table.find_all("tr")
        headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
        name_idx = headers.index("Nome")
        char_idx = headers.index("Personagem")

        current_name = current_link = None
        for i, row in enumerate(rows[1:]):
            if TEST_MODE and i >= 16: break
            if max_items and len(data) >= max_items: break

            cols = row.find_all("td")
            if not cols: continue
            values = [col.get_text(strip=True) for col in cols]
            link_tag = cols[name_idx].find("a", href=True)
            cell_text = cols[name_idx].get_text(strip=True)

            if link_tag:
                current_name = cell_text
                current_link = BASE_URL + link_tag["href"]
                char = values[char_idx] if char_idx < len(values) else ""
                if char: char_map[current_name].add(char)
            else:
                char = values[char_idx] if char_idx < len(values) else ""
                if char or cell_text:
                    char_map[current_name].add(char or cell_text)

            if link_tag or char:
                data.append(values)
                links.append(current_link if link_tag else None)

        df = pd.DataFrame(data, columns=headers)
        df["Nome"] = df["Nome"].replace("", pd.NA).ffill()
        df["URL"] = links
        df["Personagens (Todos)"] = df["Nome"].map(lambda nm: ", ".join(sorted(char_map[nm])))
    else:
        status.error("❌ Tabela não encontrada.")
        return pd.DataFrame()

    df = df[~(df["URL"].isna() & df["Personagem"].isna() & ~df["Nome"].isin(char_map))]
    df = df.drop("Personagem", axis=1)
    df["Estúdio"] = df["Estúdio"].replace("", pd.NA).ffill()
    df["Error Log"] = df["Nome"].map(error_logs).fillna("")

    expanded = []
    for _, row in df.iterrows():
        title = row["Nome"]
        msg = f"📺 Expanding: {title}"
        print(msg); status.write(msg)
        
        season_rows = get_seasons_as_rows(title, row, status)
        err = error_logs.get(title, "")
        
        # Get or fallback the original title from the current row or title
        titulo_original = row.get("Título Original", title)
        
        if season_rows:
            for s in season_rows:
                s["Error Log"] = err
                # Ensure "Título Original" is set properly
                if not s.get("Título Original"):
                    s["Título Original"] = titulo_original
                # Sanitize values
                for k, v in s.items():
                    if isinstance(v, (dict, list)):
                        s[k] = str(v)
                expanded.append(s)
        else:
            fallback = row.to_dict()
            fallback.update({
                "Título Original": titulo_original,
                "Temporada": None,
                "Ano Lançamento": None,
                "Episódios": None,
                "Error Log": err
            })
            # Sanitize values
            for k, v in fallback.items():
                if isinstance(v, (dict, list)):
                    fallback[k] = str(v)
            expanded.append(fallback)

    return pd.DataFrame(expanded)
