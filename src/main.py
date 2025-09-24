#Extraindo a API - Coinbase
import time
import requests
from datetime import datetime
from sqlalchemy import create_engine #cria a tabela
from sqlalchemy.orm import sessionmaker # cria uma sessão no db
from dotenv import load_dotenv
import os

from database import Base, BitcoinPreco

load_dotenv()

# Carrega variáveis do arquivo .env
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

DATABASE_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Cria o engine e a sessão -> Padrão SQLAlchemy
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# Função para criar uma tabela no banco, se ela não existir
def tabela_database():
    Base.metadata.create_all(engine)
    print("A tabela foi criada/verificada com sucesso!")

# modularizando a aplicação
# Extract
def extrair_dados_bitcoin():
    url = 'https://api.coinbase.com/v2/prices/spot'
    response = requests.get(url)
    dados = response.json()
    return dados

# Transform
def transforma_dados_bitcoin(dados):
    valor = dados['data']["amount"]
    criptomoeda = dados['data']["currency"]
    moeda = dados['data']["base"]
    timestamp = datetime.now()

    dados_transformados = {
        'valor':valor,
        "criptomoeda": criptomoeda,
        'moeda': moeda,
        'timestamp': timestamp
    }   

    return dados_transformados

# Carregando os dados
# Load
def salva_dados_postgres(dados):
    try:
        session = Session()
        novo_registro = BitcoinPreco(**dados)
        session.add(novo_registro)
        session.commit()
        session.close()
        print(f'[{dados["timestamp"]}] Os dados foram salvos no PostgreSQL!')
    except Exception as e:
        print(f"Ocorreu um erro: {e}")

# Basicamente, isso so executa o código só se este arquivo for rodado diretamente, e não quando for importado
if __name__ == "__main__":
    tabela_database()
    print("Iniciando ETL com atualização a cada 15 segundos... (CTRL+C para interromper)")

    while True:
        try:
            dados_json = extrair_dados_bitcoin()
            if dados_json:
                dados_tratados = transforma_dados_bitcoin(dados_json)
                print("Dados Tratados:", dados_tratados)
                salva_dados_postgres(dados_tratados)
            time.sleep(15)
        except KeyboardInterrupt:
            print("\nProcesso interrompido pelo usuário. Finalizando...")
            break
        except Exception as e:
            print(f"Erro durante a execução: {e}")
            time.sleep(15)


