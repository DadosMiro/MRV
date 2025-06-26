from MRVDataFeed import assertType
import pyodbc
from datetime import datetime
import pandas as pd

def insertClienteData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    """
    Insert customer data from all customer columns in CSV
    
    PSEUDOCODE:
    - Define customer columns: [CPF/CNPJ do Cliente, CPF Segundo Cliente, CPF Terceiro Cliente, 
                               CPF Quarto Cliente, CPF Quinto Cliente, CPF Primeiro Fiador, etc.]
    - For each customer column:
        - Clean CPF/CNPJ (remove special chars, pad CPF with zeros)
        cpfCnpj = str(row[col]).replace('.', '').replace('/', '').replace('-', '').strip()
        - Determine tipoCliente: len(11) -> 'FISICO', len(14) -> 'JURIDICO'
        - Get corresponding nome from Nome columns
        - Check if customer already exists (avoid duplicates)
        - INSERT INTO Cliente (cpfCnpj, nome, ativo, dataInclusao, dataRenovacao, tipoCliente)
        - VALUES ( nome_cliente, 1, GETDATE(), tipo_cliente)
    """
    assertType(conn, pyodbc.Connection, "Database connection")

    cursor = conn.cursor()

    Cliente = set()

    for index, row in dataChunk.iterrows():
        cpfCnpj = str(row['CPF/CNPJ do Cliente']).replace('.', '').replace('/', '').replace('-', '').strip()
        assertType(cpfCnpj, str, "CPF/CNPJ do Cliente")
        if len(cpfCnpj) == 11:
            tipoCliente = 'FISICO'
        elif len(cpfCnpj) == 14:
            tipoCliente = 'JURIDICO'
        else:
            tipoCliente =  'N/A'
    
        nome = str(row['Nome do Cliente']).strip()
        assertType(nome, str, "Nome do Cliente")
        if len(nome) == 0 or nome == "nan" or pd.isna(nome):
            nome = "N/A"
        dataInclusao = datetime.now()
        ativo = 1
        cursor.execute("SELECT ativo FROM Cliente WHERE cpfCnpj = ?", (cpfCnpj,))
        result = cursor.fetchone()
    if result is None:
        # inserir novo cliente
        cursor.execute(
            "INSERT INTO Cliente(cpfCnpj, nome, ativo, dataInclusao, tipoCliente) VALUES (?, ?, ?, ?, ?)",
            (cpfCnpj, nome, ativo, dataInclusao, tipoCliente)
        )
    elif result[0] == 0:
        # reativar
        cursor.execute(
            """
            UPDATE Cliente
            SET ativo = 1,
                dataInativacao = NULL,
                dataReativacao = ? 
            WHERE cpfCnpj = ?
            """,
            (datetime.now(), cpfCnpj)
        )
    else:
        cursor.execute(
            """
            UPDATE Cliente
            SET dataRenovacao = ?
            WHERE cpfCnpj = ?
            """,
            (datetime.now(), cpfCnpj)
        )
        Cliente.add(cpfCnpj)
    conn.commit()
    

