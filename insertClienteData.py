from MRVDataFeed import assertType
import pyodbc
from datetime import datetime
import pandas as pd
from helper import cleanCpf

def insertClienteData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    assertType(conn, pyodbc.Connection, "Database connection")
    assertType(dataChunk, pd.DataFrame, "Data chunk")

    cursor = conn.cursor()
    
    # Clean and deduplicate rows by CPF/CNPJ, keeping the first occurrence per unique CPF
    temp = dataChunk.copy()
    temp['cpfCnpj'] = temp['CPF/CNPJ do Cliente']\
        .astype(str)\
        .apply(cleanCpf)
    
    # Remove rows with invalid CPF/CNPJ
    temp = temp[temp['cpfCnpj'].notna()]
    
    # Group by cleaned CPF/CNPJ and take the first row of each group
    newChunk = temp.groupby('cpfCnpj', as_index=False).first()
    
    for _, row in newChunk.iterrows():
        # Insert main client
        insertClient('CPF/CNPJ do Cliente', 'Nome do Cliente', row, cursor, isMain=True)

        # Insert secondary clients
        sequencia = ['Segundo', 'Terceiro', 'Quarto', 'Quinto']
        for each in sequencia:
            insertClient(f'CPF {each} Cliente', f'{each if each != "Quinto" else "Quinta"} Cliente', row, cursor, isMain=False)
        
        # Insert guarantors
        sequencia = ['Primeiro', 'Segundo', 'Terceiro']
        for each in sequencia:
            insertClient(f'CPF {each} Fiador', f'{each} Fiador', row, cursor, isMain=False)
    
    conn.commit()


def insertClient(cpfF: str, nomeF: str, row: pd.Series, cursor: pyodbc.Cursor, isMain: bool):
    # Validate input parameters
    assertType(cpfF, str, "CPF field name")
    assertType(nomeF, str, "Name field name")
    assertType(row, pd.Series, "Data row")
    assertType(cursor, pyodbc.Cursor, "Database cursor")
    assertType(isMain, bool, "Main client flag")

    # Extract and clean client data
    cpfCnpj = row[cpfF]
    nome = str(row[nomeF]).strip()
    
    # Validate CPF/CNPJ for non-main clients (optional) and main clients (required)
    if not cpfCnpj and not isMain:
        return
    elif not cpfCnpj and isMain:
        raise ValueError("CPF/CNPJ do Cliente Principal não pode ser vazio ou inválido")
    
    # Validate name is not empty
    if len(nome) == 0 or nome == "nan" or pd.isna(nome):
        raise ValueError("Nome do Cliente não pode ser vazio ou inválido")

    # Check if client already exists in database
    cursor.execute("SELECT ativo, numeroIncidencia FROM Cliente WHERE cpfCnpj = ?", cpfCnpj)
    result = cursor.fetchone()
    
    if result:
        # Update existing client
        ativo, numeroIncidencia = result
        if ativo == 0:
            ativo = 1
        numeroIncidencia += 1
        dataRenovacao = datetime.now()
        sqlUpdate = """
        UPDATE Cliente 
        SET ativo = ?, numeroIncidencia = ?, dataRenovacao = ?, ehPrincipal = ?
        WHERE cpfCnpj = ?
        """
        print(f"Atualizando cliente: {nome} com CPF/CNPJ: {cpfCnpj}, ehPrincipal: {isMain}")
        cursor.execute(sqlUpdate, (ativo, numeroIncidencia, dataRenovacao, isMain, cpfCnpj))
    else:
        # Insert new client
        dataInclusao = dataRenovacao = datetime.now()
        tipoCliente = 0 if len(cpfCnpj) == 11 else 1  # 0 for CPF (11 digits), 1 for CNPJ (14 digits)
        sqlInsert = """
        INSERT INTO Cliente (cpfCnpj, nome, ativo, dataInclusao, dataRenovacao, tipoCliente)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        print(f"Inserindo cliente: {nome} com CPF/CNPJ: {cpfCnpj}, ehPrincipal: {isMain}")
        cursor.execute(sqlInsert, (cpfCnpj, nome, isMain, dataInclusao, dataRenovacao, tipoCliente))

