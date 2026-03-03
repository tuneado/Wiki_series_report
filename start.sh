#!/bin/bash
set -e

# Create virtualenv if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Criando um ambiente virtual..."
    python3 -m venv venv
fi

# Activate virtualenv
source venv/bin/activate
echo "Venv ativo!"

# Install/upgrade dependencies
pip install --upgrade pip -q
pip install -r requirements.txt -q
echo "Dependências instaladas!"

# Install Playwright browser (Chromium) if not already installed
echo "A verificar browser Playwright..."
python -m playwright install chromium
echo "Browser instalado!"

# Launch Streamlit
echo "A iniciar Streamlit..."
streamlit run app.py