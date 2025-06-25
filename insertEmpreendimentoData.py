from MRVDataFeed import assertNotNullAndType
import pyodbc
from datetime import datetime
import pandas as pd


def insertEmpreendimentoData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    """
        Codigo para inserir dados na tabela Empreendimento.
        conn: conexao atual com o banco de dados.
        dataChunk: DataFrame oriundo do csv da MRV
    """
    cursor = conn.cursor()
    for index, row in dataChunk.iterrows():
        nome = str(row['Descrição do Residencial'])
        assertNotNullAndType(nome, str, "nome do empreendimento")

        empresa = str(row['Nome da Empresa'])
        assertNotNullAndType(empresa, str, "nome da empresa")
        if len(empresa) == 0 or empresa == "nan" or pd.isna(empresa):
            empresa = "[Aviso]Dado Inconsistente"

        cnpjEmpresa = str(row['CNPJ da Empresa']).replace('.', '').replace('/', '').replace('-', '')
        assertNotNullAndType(cnpjEmpresa, str, "CNPJ da empresa")
        if len(cnpjEmpresa) != 14:
            cnpjEmpresa = None
        
        #Every new data send by MRV is considered active
        ativo = True
        
        sqlInsert = """
        INSERT INTO Empreendimento (nome, empresa, cnpjEmpresa, ativo)
        VALUES (?, ?, ?, ?)
        """

        cursor.execute(sqlInsert, (nome, empresa, cnpjEmpresa, ativo))
    conn.commit()