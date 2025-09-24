#Extraindo a API - Coinbase
import time
import requests
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os
import urllib.parse

from database import Base, BitcoinPreco

load_dotenv()
# Carrega variáveis do arquivo .env
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

# Monta a URL de conexão ao banco PostgreSQL (sem ?sslmode=...)
DATABASE_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# --- Engine e Sessão do SQLAlchemy ---
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# --- Funções do Pipeline ETL ---

def tabela_database():
    """Cria a tabela no banco de dados com base no modelo definido em `database.py`."""
    Base.metadata.create_all(engine)
    print("A tabela foi criada/verificada com sucesso!")

# [Extract] - Com tratamento de erro
def extrair_dados_bitcoin():
    """Extrai o preço do Bitcoin da API da Coinbase."""
    url = 'https://api.coinbase.com/v2/prices/spot'
    try:
        response = requests.get(url, params={'currency': 'USD'}) # Exemplo com USD
        response.raise_for_status()  # Levanta um erro para respostas HTTP ruins (4xx ou 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar a API da Coinbase: {e}")
        return None

# [Transform] - Com objeto datetime e conversão de tipo
def transforma_dados_bitcoin(dados):
    """Transforma os dados brutos da API em um formato estruturado."""
    dados_transformados = {
        'valor': float(dados['data']["amount"]),
        "criptomoeda": dados['data']["base"], # Corrigido: 'base' é a cripto (ex: BTC)
        'moeda': dados['data']["currency"],   # Corrigido: 'currency' é a moeda fiduciária (ex: USD)
        'timestamp': datetime.now()
    }
    return dados_transformados

# [Load] - Com gerenciamento de sessão moderno
def salva_dados_postgres(dados):
    """Salva os dados transformados no banco de dados PostgreSQL."""
    with Session() as session:
        try:
            novo_registro = BitcoinPreco(**dados)
            session.add(novo_registro)
            session.commit()
            print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Dados salvos: {dados["criptomoeda"]} = {dados["valor"]} {dados["moeda"]}')
        except Exception as e:
            print(f"Ocorreu um erro ao salvar no banco: {e}")
            session.rollback()

# --- Execução Principal ---
if __name__ == "__main__":
    tabela_database()
    print("Iniciando ETL com atualização a cada 15 segundos... (CTRL+C para interromper)")

    while True:
        try:
            dados_json = extrair_dados_bitcoin()
            if dados_json:
                dados_tratados = transforma_dados_bitcoin(dados_json)
                salva_dados_postgres(dados_tratados)
            
            time.sleep(15)
        except KeyboardInterrupt:
            print("\nProcesso interrompido pelo usuário. Finalizando...")
            break
        except Exception as e:
            print(f"Erro inesperado no loop principal: {e}")
            time.sleep(15) # Espera antes de tentar novamente