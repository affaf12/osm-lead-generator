mkdir -p ~/.streamlit/

echo "\
[server]\n\
headless = true\n\
enableCORS=false\n\
port = $PORT\n\
\n\
[theme]\n\
base = 'light'\n\
primaryColor = '#1E90FF'\n\
backgroundColor = '#FFFFFF'\n\
secondaryBackgroundColor = '#F0F2F6'\n\
textColor = '#000000'\n\
font = 'sans serif'\n\
" > ~/.streamlit/config.toml

# Health check to keep Streamlit Cloud app awake
while true; do
  curl -s https://$REPL_SLUG.$REPL_OWNER.repl.co/ > /dev/null 2>&1
  sleep 600
done &
