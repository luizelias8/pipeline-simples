import configparser
import requests
import pandas as pd
import datetime
import mysql.connector
from mysql.connector import Error

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
        # Conexão com o banco de dados MySQL usando os dados do arquivo .ini
        conexao = mysql.connector.connect(
            host=configuracoes_mysql['servidor'],
            user=configuracoes_mysql['usuario'],
            password=configuracoes_mysql['senha'],
            database=configuracoes_mysql['banco']
        )
        if conexao.is_connected():
            cursor = conexao.cursor()
            # Itera sobre cada linha do DataFrame e insere no banco de dados
            for _, linha in df.iterrows(): # _ é uma convenção em Python que significa que o valor não será utilizado. Nesse caso, estamos ignorando o índice da linha.
                query = f"""
                    INSERT INTO {tabela} (cidade, temperatura, umidade, descricao, data_hora)
                    VALUES (%s, %s, %s, %s, %s)
                """
                # Define os valores a serem inseridos no banco
                valores = (linha['cidade'], linha['temperatura'], linha['umidade'], linha['descricao'], linha['data_hora'])
                cursor.execute(query, valores) # Executa a inserção no banco
            conexao.commit() # Confirma a inserção
            print(f'{cursor.rowcount} registros inseridos com sucesso.')
    except Error as e:
        print(f'Erro ao conectar ao MySQL: {e}')
    finally:
        if conexao.is_connected():
            cursor.close()
            conexao.close()

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