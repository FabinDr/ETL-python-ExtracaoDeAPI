#Extraindo a API - Coinbase
import requests
from tinydb import TinyDB
from datetime import datetime

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
    timestamp = datetime.now().timestamp()

    dados_transformados = {
        'valor':valor,
        "criptomoeda": criptomoeda,
        'moeda': moeda,
        'timestamp': timestamp
    }   

    return dados_transformados

# Carregando os dados
# Load
def salva_dados_tinydb(dados, db_name='bitcoin.json'):
    try:
        db = TinyDB(db_name)
        db.insert(dados)
        print('Dados salvos com sucesso!')
    except Exception as e:
        print(f"Erro: {e}")

# Basicamente, isso so executa o código só se este arquivo for rodado diretamente, e não quando for importado
if __name__ == '__main__':
    dados_json = extrair_dados_bitcoin()
    dados_tratados = transforma_dados_bitcoin(dados_json)
    salva_dados_tinydb(dados_tratados)



