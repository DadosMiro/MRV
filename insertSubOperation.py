from MRVDataFeed import assertNotNullAndType
from concurrent.futures import ThreadPoolExecutor
import pyodbc
import pandas as pd
import sys
import numpy as np
import re
from datetime import datetime
from MRVDataFeed import InsertOperacaoData

def insertSubOperacaoData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    """
    Insert SubOperacao data linking to Operacao
    
    PSEUDOCODE:
        select 
    - For each unique combination of ( Código da Empresa, Nome da Empresa):
        - Get operacaoId from previously inserted Operacao
        - Clean CNPJ da Empresa (remove special chars)
        - INSERT INTO SubOperacao (operacaoId, codigoEmpresa, nomeEmpresaFilial, 
                                   nomeProduto, cnpjEmpresa, nomeEmpresa)
        - VALUES (operacaoId, codigo_empresa, nome_empresa, descricao_residencial, 
                  cnpj_limpo, nome_empresa)
    """

    assertNotNullAndType(conn, pyodbc.Connection, "Database connection")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 1 FROM Operacao WHERE nomeOperacao = 'MRV'
    """)
    if not cursor.fetchone():
        InsertOperacaoData(conn, dataChunk)
    # Se encontrar a informação, apenas segue o fluxo normalmente
    for index, row in dataChunk.iterrows():
        codigoEmpresa = str(row['Código da Empresa'])
        assertNotNullAndType(codigoEmpresa, str, "código da empresa")
        
        nomeEmpresa = str(row['Nome da Empresa'])
        assertNotNullAndType(nomeEmpresa, str, "nome da empresa")
        if len(nomeEmpresa) == 0 or nomeEmpresa == "nan" or pd.isna(nomeEmpresa):
            nomeEmpresa = "[Aviso]Dado Inconsistente"

        nomeProduto = str(row['Descrição do Residencial'])
        assertNotNullAndType(nomeProduto, str, "nome do produto")

        cnpjEmpresa = str(row['CNPJ da Empresa']).replace('.', '').replace('/', '').replace('-', '')
        assertNotNullAndType(cnpjEmpresa, str, "CNPJ da empresa")
        if len(cnpjEmpresa) != 14:
            cnpjEmpresa = None

        # Every new data send by MRV is considered active
        ativo = True

        sqlInsert = """
        INSERT INTO SubOperacao (codigoEmpresa, nomeEmpresaFilial, nomeProduto, cnpjEmpresa, nomeEmpresa)
        VALUES (?, ?, ?, ?, ?)
        """

        cursor.execute(sqlInsert, (codigoEmpresa, nomeEmpresa, nomeProduto, cnpjEmpresa, nomeEmpresa))
    conn.commit()
