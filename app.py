# app.py (seu Streamlit App)

import streamlit as st
import main  # importa suas funções do main.py
import pandas as pd
from io import BytesIO

st.title("Extrator de Séries")

wiki_link = st.text_input("Informe o link da Fandom:")

#Option to limit how many shows to process
st.markdown("### Opções de processamento")
parse_all = st.checkbox("Processar lista completa", value=False)

if not parse_all:
    max_items = st.number_input("Número de itens a processar:", min_value=1, step=1, value=5)
else:
    max_items = None

status = st.empty()

if st.button("Processar"):
    if not wiki_link:
        st.error("Informe um link válido.")
    else:
        with st.spinner("Processando..."):
            df = main.run_scraper(wiki_link, status=status, max_items=max_items)
        status.success("Dados processados com sucesso.")

        col1, col2= st.columns(2)
        with col1:
            st.subheader("CSV")
            # Opcional: exportar para CSV
            file_to_download = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label='Download CSV',
                data=file_to_download,
                file_name='series_temporadas_expandido.csv',
                mime='text/csv'
            )
        with col2:
            st.subheader("Excel")
             # Excel para o usuário fazer o download
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
            buffer.seek(0)

            st.download_button(
                label='Download Excel',
                data=buffer,
                file_name='series_temporadas_expandido.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        st.write("Resultado:")
        st.dataframe(df)