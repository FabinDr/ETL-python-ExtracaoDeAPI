#Extraindo a API - Coinbase
import time
import requests
from datetime import datetime
from sqlalchemy import create_engine #cria a tabela
from sqlalchemy.orm import sessionmaker # cria uma sessão no db
from dotenv import load_dotenv
import os
# Importando o logfire para observabilidade
import logging
import logfire
from logging import basicConfig, getLogger

# configuração logFire e handler
logfire_token = os.getenv("LOGFIRE_TOKEN")
logfire.configure(token=logfire_token)
basicConfig(handlers=[logfire.LogfireLoggingHandler()])
logger = getLogger(__name__)
logger.setLevel(logging.INFO)
logfire.instrument_requests()
logfire.instrument_sqlalchemy()

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
    logger.info("A tabela foi criada/verificada com sucesso!")

# modularizando a aplicação
# Extract
def extrair_dados_bitcoin():
    url = 'https://api.coinbase.com/v2/prices/spot'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Erro na API: {response.status_code}")
        return None

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
        logger.info(f'[{dados["timestamp"]}] Os dados foram salvos no PostgreSQL!')
    except Exception as e:
        logger.error(f"Erro ao inserir dados no PostgreSQL: {e}")
        session.rollback()
    finally:
        session.close()

def pipeline_bitcoin():
    with logfire.span("Executando pipeline ETL Bitcoin"):
        
        with logfire.span("Extrair Dados da API Coinbase"):
            dados_json = extrair_dados_bitcoin()
        
        if not dados_json:
            logger.error("Falha na extração dos dados. Abortando pipeline.")
            return
        
        with logfire.span("Tratar Dados do Bitcoin"):
            dados_transformados = transforma_dados_bitcoin(dados_json)
        
        with logfire.span("Salvar Dados no Postgres"):
            salva_dados_postgres(dados_transformados)

        # Exemplo de log final com placeholders
        logger.info(
            f"Pipeline finalizada com sucesso!"
        )

# Basicamente, isso so executa o código só se este arquivo for rodado diretamente, e não quando for importado
if __name__ == "__main__":
    tabela_database()
    logger.info("Iniciando ETL com atualização a cada 15 segundos... (CTRL+C para interromper)")

    while True:
        try:
            pipeline_bitcoin()
            time.sleep(15)
        except KeyboardInterrupt:
            logger.info("\nProcesso interrompido pelo usuário. Finalizando...")
            break
        except Exception as e:
            logger.info(f"Erro durante a execução: {e}")
            time.sleep(15)


