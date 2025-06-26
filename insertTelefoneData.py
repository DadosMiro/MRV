from MRVDataFeed import assertType
import pyodbc
from datetime import datetime
import pandas as pd
def insertTelefoneData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):

    assertType(conn, pyodbc.Connection, "Database connection") 
    assertType(dataChunk, pd.DataFrame, "DataFrame insertTelefoneData")

    cursor = conn.cursor()

    telefoneSet = set()

    for index, row in dataChunk.iterrows():
        cpfCnpj = cleanCpf(str(row['CPF/CNPJ do Cliente']))

        #check telefone for main client
        for i in range(1, 11):
            telefone = cleanTelefoneData(str(row[f'Telefone {i}']))
            if telefone:
                telefoneSet.add((cpfCnpj, telefone, 'NAO CLASSIFICADO'))
        

        # Secondary Clients
        result = processTelefoneSecundaryClient(row, 'CPF Segundo Cliente', 'Segundo Cliente')
        if result:
            telefoneSet.update(result)
        
        result = processTelefoneSecundaryClient(row, 'CPF Terceiro Cliente', 'Terceiro Cliente')
        if result:
            telefoneSet.update(result)
        
        result = processTelefoneSecundaryClient(row, 'CPF Quarto Cliente', 'Quarto Cliente')
        if result:
            telefoneSet.update(result)
        
        result = processTelefoneSecundaryClient(row, 'CPF Quinto Cliente', 'Quinto Cliente')
        if result:
            telefoneSet.update(result)
        
        # Guarantors
        result = processTelefoneSecundaryClient(row, 'CPF Primeiro Fiador', 'Primeiro Fiador')
        if result:
            telefoneSet.update(result)
        
        result = processTelefoneSecundaryClient(row, 'CPF Segundo Fiador', 'Segundo Fiador')
        if result:
            telefoneSet.update(result)
        
        result = processTelefoneSecundaryClient(row, 'CPF Terceiro Fiador', 'Terceiro Fiador')
        if result:
            telefoneSet.update(result)

    #all phones are considered active by default
    sqlInsert = """
    INSERT INTO Telefone (cpfCnpjCliente, numero, tipoTelefone, ativo)
    VALUES (?, ?, ?, 1);
    """
    # Insert all the set data into the database at once
    if len(telefoneSet) > 0:
        cursor.executemany(sqlInsert, telefoneSet)
        conn.commit()
    else:
        print("No phone numbers to insert.")
    # Commit transaction
    conn.commit()

    # Close the cursor
    cursor.close()



def processTelefoneSecundaryClient(row: pd.Series, cpfRowIndex: str, suffixTel: str):
    #check if there is a cpf in the row
    if pd.isna(row[cpfRowIndex]) or row[cpfRowIndex] == "nan" or len(str(row[cpfRowIndex]).strip()) == 0:
        return
    #clean the cpf
    cpfLimpo = cleanCpf(row[cpfRowIndex])
    if not cpfLimpo:
        return
    
    prefixes = ['Telefone', 'Celular', 'Telefone Comercial']
    results = set()
    for prefix in prefixes:
        telefone = cleanTelefoneData(str(row[f'{prefix} {suffixTel}']))
        #dict to hold phone data
        if telefone:
            tipoTelefone = prefix if prefix != 'Telefone' else 'Telefone FIXO'
            tel = (cpfLimpo, telefone, tipoTelefone)
            results.add((tel))
    
    return results
    
def cleanTelefoneData(tel: str):
    """
    Clean phone number by removing special characters and returning only digits.
    """
    return ''.join(filter(str.isdigit, tel))



def cleanCpf(cpf: str):
    """
    Clean CPF/CNPJ by removing special characters and returning only digits.
    """
    return ''.join(filter(str.isdigit, cpf))



if __name__ == "__main__":
    # Example usage
    
    csvData = pd.read_csv('BaseTeste.csv', encoding='utf-8', sep=';')

    insertTelefoneData(None, csvData)