from MRVDataFeed import assertType
import pyodbc
from datetime import datetime
import pandas as pd


def insertEmpreendimentoData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    """
        Codigo para inserir dados na tabela Empreendimento.
        conn: conexao atual com o banco de dados.
        dataChunk: DataFrame oriundo do csv da MRV
    """
    assertType(conn, pyodbc.Connection, "Conexão com o banco de dados")
    assertType(dataChunk, pd.DataFrame, "DataFrame com os dados do empreendimento")
    cursor = conn.cursor()

    #separar os empreendimentos únicos
    empreendimentoSet = set()

    for index, row in dataChunk.iterrows():
        nome = str(row['Descrição do Residencial'])
        assertType(nome, str, "nome do empreendimento")

        empresa = str(row['Nome da Empresa'])
        assertType(empresa, str, "nome da empresa")
        if len(empresa) == 0 or empresa == "nan" or pd.isna(empresa):
            empresa = "[Aviso]Dado Inconsistente"

        cnpjEmpresa = str(row['CNPJ da Empresa']).replace('.', '').replace('/', '').replace('-', '')
        assertType(cnpjEmpresa, str, "CNPJ da empresa")
        if len(cnpjEmpresa) != 14:
            cnpjEmpresa = None

        # Adiciona a combinação única ao conjunto
        size = len(empreendimentoSet)
        empreendimentoSet.add((nome, empresa, cnpjEmpresa))
        size2 = len(empreendimentoSet)

        if size == size2:
            continue

        # Todo novo empreendimento é considerado ativo
        ativo = True

        sqlInsert = """
        INSERT INTO Empreendimento (nome, empresa, cnpjEmpresa, ativo)
        VALUES (?, ?, ?, ?)
        """
        cursor.execute(sqlInsert, (nome, empresa, cnpjEmpresa, ativo))

    # Removendo duplicatas ,se quiser colocar ao final de todas as insercoes (main code)
    sqlDropDuplicates = """
    DELETE FROM Empreendimento 
    WHERE id IN (
        SELECT id FROM (
            SELECT id,
                   ROW_NUMBER() OVER (PARTITION BY nome, empresa, cnpjEmpresa ORDER BY id) AS rn
            FROM Empreendimento
        ) AS CTE 
        WHERE rn > 1
    )
    """
    cursor.execute(sqlDropDuplicates)

    # Commit transaction
    conn.commit()