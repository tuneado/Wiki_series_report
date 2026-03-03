# app.py — Streamlit UI for Wikidobragens Series Report

import re
import streamlit as st
import main
import pandas as pd
from io import BytesIO

st.set_page_config(page_title="Wikidobragens Info", page_icon="📺", layout="wide")
st.title("Wikidobragens Info de Séries")

# Initialize session state for persisting data
if "df" not in st.session_state:
    st.session_state.df = None
if "error_logs" not in st.session_state:
    st.session_state.error_logs = {}

wiki_link = st.text_input("O seu link da Wiki:")

# --- Options ---
st.markdown("### Opções")
col_opt1, col_opt2, col_opt3 = st.columns(3)
with col_opt1:
    process_mode = st.radio(
        "Modo de processamento:",
        ["Primeiros N itens", "Intervalo (de/até)", "Lista completa"],
        index=0,
    )
with col_opt2:
    if process_mode == "Primeiros N itens":
        max_items = st.number_input("Número de itens a processar:", min_value=1, step=1, value=5)
        start_item = 1
    elif process_mode == "Intervalo (de/até)":
        start_item = st.number_input("Processar itens de:", min_value=1, step=1, value=1)
        end_item = st.number_input("até:", min_value=start_item, step=1, value=start_item + 4)
        max_items = end_item  # parse up to this many from the table
    else:
        max_items = None
        start_item = 1
with col_opt3:
    content_type = st.radio(
        "Tipo de conteúdo:",
        ["Séries", "Filmes", "Ambos"],
        index=0,
    )
    # Convert to flags for run_scraper
    include_series = content_type in ["Séries", "Ambos"]
    include_films = content_type in ["Filmes", "Ambos"]

status = st.empty()
progress_bar = st.progress(0)

FANDOM_URL_PATTERN = re.compile(r"^https?://wikidobragens\.fandom\.com(/[a-z]{2})?/wiki/.+")

if st.button("Processar"):
    if not wiki_link:
        st.error("Insira um link válido.")
    elif not FANDOM_URL_PATTERN.match(wiki_link):
        st.error("❌ O link deve ser do formato: https://wikidobragens.fandom.com/wiki/...")
    else:
        with st.spinner("Processando..."):
            df = main.run_scraper(
                wiki_link,
                status=status,
                max_items=max_items,
                start_item=start_item,
                include_series=include_series,
                include_films=include_films,
                progress_bar=progress_bar,
            )
        progress_bar.progress(1.0)
        status.success("✅ Dados processados com sucesso.")
        
        # Store in session state to persist across reruns
        st.session_state.df = df
        st.session_state.error_logs = dict(main.error_logs)

# --- Display results (outside button handler to persist) ---
if st.session_state.df is not None:
    df = st.session_state.df
    
    # --- Error log display ---
    if st.session_state.error_logs:
        with st.expander(f"⚠️ {len(st.session_state.error_logs)} erro(s) encontrados", expanded=False):
            for show, err in st.session_state.error_logs.items():
                st.warning(f"**{show}**: {err}")
    
    # --- Column picker for export ---
    if not df.empty:
        all_columns = df.columns.tolist()
        selected_columns = st.multiselect(
            "Colunas a exportar:",
            options=all_columns,
            default=all_columns,
            key="column_selector",  # Unique key to prevent reset issues
        )
        export_df = df[selected_columns].copy() if selected_columns else df.copy()

        # Fix "Total Episódios" to display as integers (not floats like 52.0)
        if "Total Episódios" in export_df.columns:
            export_df["Total Episódios"] = export_df["Total Episódios"].apply(
                lambda x: int(x) if isinstance(x, (int, float)) and x == x and str(x) != "N/A" else x
            )

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("CSV")
            file_to_download = export_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="Download CSV",
                data=file_to_download,
                file_name="series_temporadas_expandido.csv",
                mime="text/csv",
            )
        with col2:
            st.subheader("Excel")
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                export_df.to_excel(writer, index=False)
            buffer.seek(0)

            st.download_button(
                label="Download Excel",
                data=buffer,
                file_name="series_temporadas_expandido.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        st.write("Resultado:")
        
        # Apply color styling based on Match Confidence
        def style_confidence(val):
            colors = {
                "exact": "background-color: #90EE90",    # Light green
                "high": "background-color: #98FB98",     # Pale green
                "medium": "background-color: #FFFFE0",   # Light yellow
                "low": "background-color: #FFB6C1",      # Light pink
                "none": "background-color: #FF6B6B",     # Red
            }
            return colors.get(val, "")
        
        # Hide Match Score from display (keep it for export)
        display_df = df.drop(columns=["Match Score"], errors="ignore")
        
        if "Match Confidence" in display_df.columns:
            styled_df = display_df.style.map(style_confidence, subset=["Match Confidence"])
            st.dataframe(styled_df, use_container_width=True)
        else:
            st.dataframe(display_df, use_container_width=True)
    else:
        st.warning("Nenhum dado encontrado.")