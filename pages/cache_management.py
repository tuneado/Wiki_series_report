# -*- coding: utf-8 -*-
"""
Cache Management Page — Admin interface for Supabase cache.

Allows viewing cache statistics, searching entries, and clearing cache.
Password protected - only admin can access.
"""

import os
import streamlit as st
import cache as db_cache
from dotenv import load_dotenv

load_dotenv("vars.env")

st.set_page_config(page_title="Cache Management", page_icon="🗄️", layout="wide")

# --- Password Protection ---
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")
if not ADMIN_PASSWORD:
    try:
        ADMIN_PASSWORD = st.secrets["ADMIN_PASSWORD"]
    except (KeyError, FileNotFoundError):
        ADMIN_PASSWORD = "admin123"  # Default for testing

# Initialize session state for authentication
if "admin_authenticated" not in st.session_state:
    st.session_state.admin_authenticated = False

def check_password():
    """Show login form and verify password."""
    if st.session_state.admin_authenticated:
        return True
    
    st.title("🔐 Acesso Restrito")
    st.warning("Esta página requer autenticação de administrador.")
    
    with st.form("login_form"):
        password = st.text_input("Password:", type="password")
        submit = st.form_submit_button("Entrar")
        
        if submit:
            if password == ADMIN_PASSWORD:
                st.session_state.admin_authenticated = True
                st.rerun()
            else:
                st.error("❌ Password incorreta.")
    
    return False

# Check authentication before showing content
if not check_password():
    st.stop()

# --- Admin Content (only shown after authentication) ---
st.title("🗄️ Gestão de Cache")

# Logout button
col_logout, col_spacer = st.columns([1, 5])
with col_logout:
    if st.button("🚪 Logout"):
        st.session_state.admin_authenticated = False
        st.rerun()

st.markdown("""
Esta página permite gerir a cache Supabase que armazena dados de Fandom, IMDb e TMDb.
A cache acelera pesquisas futuras ao evitar chamadas repetidas às APIs.
""")

# --- Connection Status ---
st.header("🔌 Estado da Ligação")

# Check environment variables
supabase_url = os.getenv("SUPABASE_URL") or db_cache.SUPABASE_URL
supabase_key = os.getenv("SUPABASE_KEY") or db_cache.SUPABASE_KEY

col_status1, col_status2 = st.columns(2)

with col_status1:
    st.subheader("Configuração")
    if supabase_url:
        st.success(f"✅ SUPABASE_URL: `{supabase_url[:40]}...`")
    else:
        st.error("❌ SUPABASE_URL: Não configurado")
    
    if supabase_key:
        if supabase_key.startswith("eyJ"):
            st.success(f"✅ SUPABASE_KEY: `{supabase_key[:30]}...` (JWT válido)")
        else:
            st.error(f"❌ SUPABASE_KEY: `{supabase_key[:20]}...` (formato inválido - deve começar com 'eyJ')")
            st.info("💡 Vá a Supabase → Settings → API → copie a chave 'anon public' (a longa)")
    else:
        st.error("❌ SUPABASE_KEY: Não configurado")

with col_status2:
    st.subheader("Teste de Ligação")
    
    # Test actual connection
    if st.button("🔄 Testar Ligação"):
        client = db_cache._get_client()
        if client:
            try:
                # Try to query a table
                result = client.table("fandom_cache").select("url").limit(1).execute()
                st.success("✅ Ligação bem sucedida! Base de dados acessível.")
            except Exception as e:
                error_str = str(e)
                if "relation" in error_str and "does not exist" in error_str:
                    st.error("❌ Tabelas não existem. Execute o SQL de criação no Supabase.")
                    with st.expander("Ver SQL para criar tabelas"):
                        st.code("""
CREATE TABLE fandom_cache (
    url TEXT PRIMARY KEY,
    titulo_original TEXT,
    direcao_atores TEXT,
    direcao_tecnica TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE imdb_cache (
    search_title TEXT PRIMARY KEY,
    imdb_id TEXT,
    matched_title TEXT,
    kind TEXT,
    confidence_level TEXT,
    confidence_score REAL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE tmdb_shows (
    imdb_id TEXT PRIMARY KEY,
    tmdb_id INTEGER,
    original_name TEXT,
    seasons_json JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE tmdb_movies (
    imdb_id TEXT PRIMARY KEY,
    tmdb_id INTEGER,
    original_title TEXT,
    release_year TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
                        """, language="sql")
                elif "Invalid API key" in error_str or "invalid" in error_str.lower():
                    st.error(f"❌ Chave API inválida: {error_str}")
                else:
                    st.error(f"❌ Erro: {error_str}")
        else:
            st.error("❌ Não foi possível criar cliente Supabase. Verifique as credenciais.")

st.divider()

# --- Cache Statistics ---
st.header("📊 Estatísticas")

stats = db_cache.get_cache_stats()

if not stats["connected"]:
    st.error("❌ Supabase não está configurado. Adicione `SUPABASE_URL` e `SUPABASE_KEY` ao ficheiro `vars.env`.")
else:
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Fandom Pages", stats["fandom_cache"])
    with col2:
        st.metric("IMDb Lookups", stats["imdb_cache"])
    with col3:
        st.metric("TMDb Shows", stats["tmdb_shows"])
    with col4:
        st.metric("TMDb Movies", stats["tmdb_movies"])
    with col5:
        st.metric("Total Entries", stats["total"])

st.divider()

# --- Search Cache ---
st.header("🔍 Pesquisar Cache")

search_query = st.text_input("Pesquisar por título ou URL:", placeholder="Ex: Spider-Man, Avatar...")

if search_query:
    results = db_cache.search_cache(search_query, limit=50)
    if results:
        st.success(f"Encontrados {len(results)} resultados")
        
        # Group by type
        for result in results:
            with st.expander(f"[{result['type'].upper()}] {result['title']}"):
                st.write(f"**Chave:** `{result['key']}`")
                if result.get("imdb_id"):
                    st.write(f"**IMDb ID:** {result['imdb_id']}")
                if result.get("tmdb_id"):
                    st.write(f"**TMDb ID:** {result['tmdb_id']}")
                if result.get("created_at"):
                    st.write(f"**Criado em:** {result['created_at']}")
                
                # Delete button for individual entry
                table_map = {
                    "fandom": "fandom_cache",
                    "imdb": "imdb_cache",
                    "tmdb_show": "tmdb_shows",
                    "tmdb_movie": "tmdb_movies",
                }
                table = table_map.get(result["type"])
                if st.button(f"🗑️ Apagar", key=f"delete_{result['type']}_{result['key'][:20]}"):
                    if db_cache.delete_cache_entry(table, result["key"]):
                        st.success("Entrada apagada!")
                        st.rerun()
                    else:
                        st.error("Erro ao apagar entrada")
    else:
        st.info("Nenhum resultado encontrado.")

st.divider()

# --- Clear Cache ---
st.header("🧹 Limpar Cache")

st.warning("⚠️ Atenção: Limpar a cache irá forçar novas pesquisas nas APIs, tornando o processamento mais lento temporariamente.")

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    if st.button("Limpar Fandom", type="secondary"):
        if db_cache.clear_cache("fandom_cache"):
            st.success("Cache Fandom limpa!")
            st.rerun()

with col2:
    if st.button("Limpar IMDb", type="secondary"):
        if db_cache.clear_cache("imdb_cache"):
            st.success("Cache IMDb limpa!")
            st.rerun()

with col3:
    if st.button("Limpar TMDb Shows", type="secondary"):
        if db_cache.clear_cache("tmdb_shows"):
            st.success("Cache TMDb Shows limpa!")
            st.rerun()

with col4:
    if st.button("Limpar TMDb Movies", type="secondary"):
        if db_cache.clear_cache("tmdb_movies"):
            st.success("Cache TMDb Movies limpa!")
            st.rerun()

with col5:
    if st.button("🗑️ LIMPAR TUDO", type="primary"):
        if db_cache.clear_cache():
            st.success("Toda a cache foi limpa!")
            st.rerun()

st.divider()

# --- View Recent Entries ---
st.header("📋 Entradas Recentes")

if stats["connected"] and stats["total"] > 0:
    tab1, tab2, tab3, tab4 = st.tabs(["Fandom", "IMDb", "TMDb Shows", "TMDb Movies"])
    
    with tab1:
        entries = db_cache.get_recent_entries("fandom_cache", limit=10)
        if entries:
            for entry in entries:
                with st.expander(f"🌐 {entry.get('url', 'N/A')[:60]}..."):
                    st.write(f"**Título Original:** {entry.get('titulo_original', 'N/A')}")
                    st.write(f"**Direção de Atores:** {entry.get('direcao_atores', 'N/A')}")
                    st.write(f"**Direção Técnica:** {entry.get('direcao_tecnica', 'N/A')}")
                    st.write(f"**Criado:** {entry.get('created_at', 'N/A')}")
        else:
            st.info("Sem entradas na cache Fandom.")
    
    with tab2:
        entries = db_cache.get_recent_entries("imdb_cache", limit=10)
        if entries:
            for entry in entries:
                with st.expander(f"🎬 {entry.get('search_title', 'N/A')} → {entry.get('matched_title', 'N/A')}"):
                    st.write(f"**Pesquisa:** {entry.get('search_title', 'N/A')}")
                    st.write(f"**Título Encontrado:** {entry.get('matched_title', 'N/A')}")
                    st.write(f"**IMDb ID:** {entry.get('imdb_id', 'N/A')}")
                    st.write(f"**Tipo:** {entry.get('kind', 'N/A')}")
                    st.write(f"**Confiança:** {entry.get('confidence_level', 'N/A')} ({entry.get('confidence_score', 'N/A')})")
                    st.write(f"**Criado:** {entry.get('created_at', 'N/A')}")
        else:
            st.info("Sem entradas na cache IMDb.")
    
    with tab3:
        entries = db_cache.get_recent_entries("tmdb_shows", limit=10)
        if entries:
            for entry in entries:
                with st.expander(f"📺 {entry.get('original_name', 'N/A')}"):
                    st.write(f"**Nome Original:** {entry.get('original_name', 'N/A')}")
                    st.write(f"**IMDb ID:** {entry.get('imdb_id', 'N/A')}")
                    st.write(f"**TMDb ID:** {entry.get('tmdb_id', 'N/A')}")
                    seasons = entry.get('seasons_json', [])
                    if seasons:
                        st.write(f"**Temporadas:** {len(seasons)}")
                    st.write(f"**Criado:** {entry.get('created_at', 'N/A')}")
        else:
            st.info("Sem entradas na cache TMDb Shows.")
    
    with tab4:
        entries = db_cache.get_recent_entries("tmdb_movies", limit=10)
        if entries:
            for entry in entries:
                with st.expander(f"🎥 {entry.get('original_title', 'N/A')}"):
                    st.write(f"**Título Original:** {entry.get('original_title', 'N/A')}")
                    st.write(f"**IMDb ID:** {entry.get('imdb_id', 'N/A')}")
                    st.write(f"**TMDb ID:** {entry.get('tmdb_id', 'N/A')}")
                    st.write(f"**Ano:** {entry.get('release_year', 'N/A')}")
                    st.write(f"**Criado:** {entry.get('created_at', 'N/A')}")
        else:
            st.info("Sem entradas na cache TMDb Movies.")
else:
    st.info("Cache vazia ou não conectada.")

st.divider()

# --- Info ---
st.header("ℹ️ Informação")

st.markdown("""
### Como funciona a cache?

1. **Fandom Cache**: Guarda dados extraídos de páginas Fandom (Título Original, Direção de Atores, etc.)
2. **IMDb Cache**: Guarda resultados de pesquisas IMDb (IMDb ID, título encontrado, tipo)
3. **TMDb Shows**: Guarda informação de séries (temporadas, anos, episódios)
4. **TMDb Movies**: Guarda informação de filmes (título original, ano de lançamento)

### Quando limpar a cache?

- **Dados incorretos**: Se um título foi associado incorretamente
- **Dados desatualizados**: Se uma série tem novas temporadas
- **Problemas de pesquisa**: Se uma pesquisa falhou mas deveria ter funcionado

### Notas

- A cache não expira automaticamente — os dados ficam guardados até serem apagados manualmente
- Todos os utilizadores partilham a mesma cache
- Primeira pesquisa de um título: ~5-10s | Pesquisa com cache: <1s
""")
