import streamlit as st
import psycopg2
import pandas as pd
import time
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

@st.cache_data(ttl=60) # Para deixar o dash mais rápido

def ler_dados():
    try:
        conn = psycopg2.connect(
            host = POSTGRES_HOST,
            database = POSTGRES_DB,
            user = POSTGRES_USER,
            password = POSTGRES_PASSWORD,
            port= POSTGRES_PORT,
            sslmode='require'
        )
        query = 'SELECT * FROM bitcoin_precos ORDER BY timestamp DESC'
        df = pd.read_sql(query, conn)
        conn.close()
        
        return df
    except Exception as e:
        st.error(f'Erro ao conectar com o Banco: {e}')
        return pd.DataFrame()

def main():
    st.set_page_config(page_title="Dashboard de Preços do Bitcoin", layout="wide")
    st.title("Dashboard de Preços do Bitcoin")
    st.write("Este dashboard exibe os dados do preço do Bitcoin coletados periodicamente em um banco PostgreSQL.")

    df = ler_dados()

    if not df.empty:
        #Kpi´s
        st.subheader("🔢 Estatísticas Gerais")
        col1, col2, col3 = st.columns(3)
        col1.metric("Preço Atual", f"${df['valor'].iloc[-1]:,.2f}")
        col2.metric("Preço Máximo (registrado)", f"${df['valor'].max():,.2f}")
        col3.metric("Preço Mínimo (registrado)", f"${df['valor'].min():,.2f}")

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(by='timestamp')
        
        st.subheader("📈 Evolução do Preço do Bitcoin")
        st.line_chart(data=df, x='timestamp', y='valor', use_container_width=True)
    else:
        st.warning("Nenhum dado encontrado no banco de dados PostgreSQL.")

        st.subheader("Dados Detalhados")

    with st.expander("📋 Tabela com os Dados Recentes"):
        default_cols = list(df)
        # Limita a exibição para não travar
        st.dataframe(df)
        st.caption(f"Mostrando até 1000 de {len(df)} registros.")

if __name__ == "__main__":
    main()

