import configparser
import requests
import pandas as pd
import datetime
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Função para ler o arquivo .ini e obter as configurações
def ler_configuracoes(arquivo_config):
    configuracao = configparser.ConfigParser()
    configuracao.read(arquivo_config)
    return configuracao

# Função para extrair dados da API OpenWeatherMap
def extrair_dados_api(chave_api, cidade):
    # Monta a URL para fazer a requisição à API com a cidade e a chave da API
    url = f'http://api.openweathermap.org/data/2.5/weather?q={cidade}&appid={chave_api}&lang=pt_br&units=metric'
    try:
        resposta = requests.get(url)
        resposta.raise_for_status() # Levanta exceção para erros HTTP
        dados = resposta.json()
        return dados
    except requests.exceptions.RequestException as e:
        print(f'Erro ao acessar a API: {e}')
        return None

# Função para transformar e tratar os dados
def transformar_dados(dados):
    try:
        # Extrai os dados desejados e os armazena em um dicionário
        dados_tratados = {
            'cidade': dados['name'],
            'temperatura': dados['main']['temp'],
            'umidade': dados['main']['humidity'],
            'descricao': dados['weather'][0]['description'],
            'data_hora': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        # Cria um DataFrame a partir do dicionário, envolvendo com colchetes para transformar em uma lista de dicionários
        df = pd.DataFrame([dados_tratados])
        return df
    except Exception as e:
        print(f'Erro ao transformar os dados: {e}')
        return None

# Função para carregar dados no MySQL
def carregar_dados_mysql(df, tabela, configuracoes_mysql):
    try:
        # Criar a string de conexão com o banco de dados MySQL usando SQLAlchemy
        usuario = configuracoes_mysql['usuario']
        senha = configuracoes_mysql['senha']
        servidor = configuracoes_mysql['servidor']
        banco = configuracoes_mysql['banco']
        url_conexao = f'mysql+pymysql://{usuario}:{senha}@{servidor}/{banco}'

        # Criar o engine de conexão com o banco de dados
        engine = create_engine(url_conexao)

        # Carregar os dados no MySQL
        df.to_sql(tabela, con=engine, if_exists='append', index=False)
        print(f'{len(df)} registros inseridos com sucesso.')

    except SQLAlchemyError as e:
        print(f'Erro ao conectar ao MySQL usando SQLAlchemy: {e}')

# Função principal do pipeline
def executar_pipeline():
    configuracao = ler_configuracoes('config.ini')
    chave_api = configuracao['api']['chave_api']
    cidade = configuracao['api']['cidade']
    dados = extrair_dados_api(chave_api, cidade)
    if dados:
        df_tratado = transformar_dados(dados)
        if df_tratado is not None:
            configuracoes_mysql = configuracao['mysql']
            carregar_dados_mysql(df_tratado, 'clima', configuracoes_mysql)

if __name__ == '__main__':
    executar_pipeline()