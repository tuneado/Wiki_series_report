# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from collections import defaultdict
from imdb import Cinemagoer
import tmdbsimple as tmdb
from dotenv import load_dotenv
import os
import streamlit as st

load_dotenv()

API_KEY = os.getenv("TMDB_API_KEY") #API KEY STORED IN .env

# Fallback to Streamlit secrets if not found
if not API_KEY:
    if "TMDB_API_KEY" in st.secrets:
        API_KEY = st.secrets["TMDB_API_KEY"]
    else:
        st.error("❌ No TMDB API KEY defined in .env or Streamlit secrets.")
        st.stop()



BASE_URL = "https://wikidobragens.fandom.com"

TEST_MODE = False  # ⬅️ Set to False to run on full data

#Initialize a list to hold error messages for each row
error_logs = []

# --- Utility Function: Extract Info by Label from a Detail Page ---
def extract_labels_from_page(url, labels):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        data_blocks = soup.find_all("div", class_="pi-data")

        result = {}
        for block in data_blocks:
            label_tag = block.find("h3", class_="pi-data-label")
            value_tag = block.find("div", class_="pi-data-value")
            if label_tag and value_tag:
                label_text = label_tag.get_text(strip=True)
                value_text = value_tag.get_text(strip=True)
                if label_text in labels:
                    result[label_text] = value_text
        return result

    except Exception as e:
        print(f"❌ Error accessing {url}: {e}")
        return {label: None for label in labels}


def get_seasons_as_rows(show_title, base_row , status=None):
    fandom_url = base_row['URL']
    if fandom_url:
        print((msg := f"🔍 Extraindo info da Fandom para: {show_title}")); status.write(msg)
        extra_data = extract_labels_from_page(fandom_url , ["Direção de Atores"])

        direction = extra_data.get("Direção de Atores") or ""

        base_row = base_row.to_dict()
        base_row["Direção de Atores"] = direction
    # Initialize IMDbPY
    ia = Cinemagoer()
    print((msg := f"🔍 Searching IMDb for: {show_title}")); status.write(msg);
    # Search for the show on IMDb
    results = ia.search_movie(show_title)

    if not results:
        print("  ❌ No results found on IMDb.")
        error_logs.append(" ❌ No results found on IMDb.")
        return []
    # Iterate through results to find the first TV series
    for result in results:
        ia.update(result)
        if result.get('kind') == 'tv series':
            show = result


            break
    else:
        print("  ❌ No TV series found in the results.")
        return []

    ia.update(show)  # Update the show info with IMDbPY
    original_show_name = show.get('title')
    print((msg := f"  🎬 Found: {original_show_name} ({show.movieID}) — Type: {show.get('kind')}")); status.write(msg);
    # Step 1: Search TMDb for show
    search_url = "https://api.themoviedb.org/3/search/tv"
    search_resp = requests.get(search_url, params={"api_key": API_KEY, "query": original_show_name}).json()
    results = search_resp.get("results", [])

    if not results:
        print(f"❌ Show not found: {show_title}")
        return []

    show_id = results[0]["id"]

    # Step 2: Get full show info
    show_url = f"https://api.themoviedb.org/3/tv/{show_id}"
    show_resp = requests.get(show_url, params={"api_key": API_KEY}).json()

    rows = []
    for season in show_resp.get("seasons", []):
        season_number = season.get("season_number")
        air_date = season.get("air_date")
        episode_count = season.get("episode_count", 0)
        release_year = air_date[:4] if air_date else "N/A"

        new_row = dict(base_row)
        new_row["Nome Original"] = original_show_name
        new_row["Temporada"] = "Especiais" if season_number == 0 else season_number
        new_row["Ano Lançamento"] = release_year
        new_row["Episódios"] = episode_count
        rows.append(new_row)

    return rows



def run_scraper(wiki_link , status=None , max_items=None):
    # --- Step 1: Fetch the main page and find the table ---
    #main_url = f"{BASE_URL}/pt/wiki/André_Raimundo"
    response = requests.get(wiki_link)
    soup = BeautifulSoup(response.content, "html.parser")

    print((msg := "A procurar tabela na Fandom...")); status.write(msg)

    # Find section <span class="mw-headline">Séries</span>
    target_section = soup.find("span", class_="mw-headline", string="Séries")
    table = None
    if target_section:
        current = target_section.parent
        while current:
            current = current.find_next_sibling()
            if current and current.name == "table" and "article-table" in current.get("class", []):
                table = current
                break

    # --- Step 2: Extract table data and links ---
    data = []
    char_map = defaultdict(set)
    links = []

    if table:
        rows = table.find_all("tr")
        headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]

        name_col_index = headers.index("Nome")
        personagem_col_index = headers.index("Personagem")
        estudio_col_index = headers.index("Estúdio")

        current_name = None
        current_link = None

        for i, row in enumerate(rows[1:]):
            if TEST_MODE and i >= 16:
                break
            if max_items and len(data) >= max_items:
                break
            cols = row.find_all("td")
            if not cols:
                continue

            row_values = [col.get_text(strip=True) for col in cols]
            first_cell = cols[name_col_index]
            link_tag = first_cell.find("a", href=True)
            cell_text = first_cell.get_text(strip=True)

            if link_tag:
                # This row starts a new show:
                current_name = cell_text
                current_link = BASE_URL + link_tag["href"]
                # Also collect персонаgm from the designated column, if any
                personagem_text = (
                    row_values[personagem_col_index]
                    if personagem_col_index < len(row_values) else ""
                )
                if personagem_text:
                    char_map[current_name].add(personagem_text)

            else:
                # No link in first cell → likely a character‐only row
                # If the “Personagem” column has something, take that first:
                personagem_text = (
                    row_values[personagem_col_index]
                    if personagem_col_index < len(row_values) else ""
                )

                if personagem_text:
                    char_map[current_name].add(personagem_text)
                elif cell_text:
                    # If “Personagem” is empty but first cell has text,
                    # treat that text as a character name:
                    char_map[current_name].add(cell_text)
                # else: truly a blank row with no character

            # Decide whether to keep this row in the DataFrame
            is_character_row = bool(personagem_text or (cell_text and not link_tag))

            if link_tag or is_character_row:
                data.append(row_values)
                links.append(current_link if link_tag else None)
            # else: skip ghost rows like "Will Benjamin" without link or character



        # Now build df
        df = pd.DataFrame(data, columns=headers)
        df["Nome"] = df["Nome"].replace("", pd.NA).ffill()
        df["URL"] = links
        df["Personagens (Todos)"] = df["Nome"].map(lambda nm: sorted(char_map.get(nm, []))).apply(lambda x: ", ".join(map(str, x)))
        #df["Personagens (Todos)"] = df["Personagens (Todos)"].apply(lambda x: ", ".join(map(str, x)))

    else:
        df = pd.DataFrame()
        print((msg := "❌ Table not found in 'Séries' section.")); status.error(msg); error_logs.append(msg)

    # Remove rows that are likely junk: no link, no personagem, and name isn't a known show
    df = df[~(
        df["URL"].isna() &
        df["Personagem"].isna() &
        ~df["Nome"].isin(char_map.keys())
    )]
    df = df.drop("Personagem", axis=1)


    # --- Apply to DataFrame ---
    for title in df['Nome']:
        print((msg := f"🔎 Searching IMDb for: {title}")); status.write(msg); 
        #n_seasons , seasons_date , tmdb_link , original_title = get_tmdb_season_info(title)
        #print(f"📺 {title}: {n_seasons} season(s)")
        #season_counts.append(n_seasons)
        #show_nome_original.append(original_title)
        #time.sleep(1)  # Be nice to IMDb's servers

    cols_to_fill = ["Estúdio"]
    df[cols_to_fill] = df[cols_to_fill].replace("", pd.NA).ffill()

    expanded_rows = []

    for _, row in df.iterrows():
        title = row["Nome"]
        print((msg := f"📺 Expanding: {title}")); status.write(msg);
        season_rows = get_seasons_as_rows(title, row , status)

        if season_rows:
            expanded_rows.extend(season_rows)
        else:
            fallback_row = row.copy()
            fallback_row["Temporada"] = None
            fallback_row["Ano Lançamento"] = None
            fallback_row["Episódios"] = None
            expanded_rows.append(fallback_row)

    if error_logs:
      df["Error Log"] = error_logs
    df_expanded = pd.DataFrame(expanded_rows)


    return df_expanded

    """

    # --- Step 4: Export ---
    df.to_excel("tabela_series_com_info.xlsx", index=False)
    print("✅ Excel file saved as 'tabela_series_com_info.xlsx'")

    df_expanded.to_excel("series_temporadas_expandido.xlsx", index=False)
    print("✅ Saved as 'series_temporadas_expandido.xlsx'")

    """