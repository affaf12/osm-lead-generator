#!/bin/bash

echo "🚀 Setting up Streamlit environment..."

# Create Streamlit config directory
mkdir -p ~/.streamlit/

# Write config.toml with optimized server + theme settings
cat <<EOF > ~/.streamlit/config.toml
[server]
headless = true
enableCORS = false
enableXsrfProtection = false
port = \$PORT

[theme]
base = "light"
primaryColor = "#1E90FF"
backgroundColor = "#FFFFFF"
secondaryBackgroundColor = "#F0F2F6"
textColor = "#000000"
font = "sans serif"

[browser]
gatherUsageStats = false
EOF

# Upgrade pip & build tools for faster package installs
echo "⬆️ Upgrading pip, setuptools, and wheel..."
python -m pip install --upgrade pip setuptools wheel

# Optional: pre-install key libraries to speed up deployment
echo "⚙️ Pre-installing common dependencies..."
pip install --no-cache-dir streamlit pandas requests beautifulsoup4 folium openpyxl

echo "✅ Setup complete!"
