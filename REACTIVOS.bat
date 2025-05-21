@echo off
REM Ejecuta Streamlit completamente en segundo plano sin mostrar CMD
start "" /B pythonw -m streamlit run code.py
exit