from MRVDataFeed import assertType
import pyodbc
from datetime import datetime
import pandas as pd
from insertApiCep import consultaCep
from helper import cleanCep


def insertEnderecoData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    assertType(conn, pyodbc.Connection, "Database connection")
    assertType(dataChunk, pd.DataFrame, "Data chunk")

    cursor = conn.cursor()
    #fazer a consulta por ceps unicos no banco, usar dados comuns de cep como cache
    #o(1)

    for index, row in dataChunk.iterrows():
        cep = str(row.get('CEP do Cliente', '')).
        if len(cep) != 8 or not cep.isdigit():

        logradouro = str(row.get('Endereço do Cliente', '')).strip()
        bairro = str(row.get('Bairro do Cliente', '')).strip()

        endereco_via_cep = None
        if not logradouro or logradouro.lower() == "nan" or pd.isna(logradouro):
            endereco_via_cep = consultaCep(cep)
            if endereco_via_cep:
                logradouro = endereco_via_cep.get("logradouro", "N/A").strip().title()      
        if not bairro or bairro.lower() == "nan" or pd.isna(bairro):
            if not endereco_via_cep:
                endereco_via_cep = consultaCep(cep)
            if endereco_via_cep:
                bairro = endereco_via_cep.get("bairro", "N/A").strip().title()
            else:
                bairro = "N/A"

        insert_sql = """
            INSERT INTO EnderecoCliente (cep, logradouro, bairro)
            VALUES (?, ?, ?)
        """
        cursor.execute(insert_sql, cep, logradouro, bairro)

    conn.commit()
    cursor.close()

    """
    Insert address data from customer address fields
    
    PSEUDOCODE:
    Função insertEnderecoData(conexao, dados_em_tabela):
    Validar se conexao é do tipo correto
    Validar se dados_em_tabela é um DataFrame

    Criar cursor para executar comandos SQL

    Para cada linha em dados_em_tabela:
        Extrair o CEP do cliente e remover pontos, traços e espaços
        Verificar se o CEP tem 8 dígitos numéricos
            Se não tiver, marcar como "N/A"

        Extrair o logradouro (endereço) da linha e remover espaços
        Se o logradouro estiver vazio ou inválido:
            Consultar API de CEP
            Se a resposta for válida:
                Obter o logradouro da API

        Extrair o bairro da linha e formatar
        Se o bairro estiver vazio ou inválido:
            Se ainda não consultou o CEP, consultar agora
            Se a resposta for válida:
                Obter o bairro da API

        Preparar o comando SQL para inserir os dados na tabela EnderecoCliente
        Tentar executar a inserção com os valores: CEP, logradouro e bairro
        Se houver erro, imprimir o erro e continuar

    Confirmar todas as inserções no banco (commit)
    Exibir mensagem de sucesso

    """
