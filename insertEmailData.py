from MRVDataFeed import assertType
import pyodbc
import pandas as pd
from helper import cleanCpf, cleanEmail

def insertEmailData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):

    assertType(conn, pyodbc.Connection, "Database connection") 
    assertType(dataChunk, pd.DataFrame, "DataFrame insertEmail")

    cursor = conn.cursor()

    emailSet = set()

    for _, row in dataChunk.iterrows():
        cpfCnpj = cleanCpf(str(row['CPF/CNPJ do Cliente']))

        #check email for main client
        email = cleanEmail(str(row['E-mail do Cliente']))

        if email:
            emailSet.add((cpfCnpj, email))
        

        # Secondary Clients
        result = processEmailSecundaryClient(row, 'CPF Segundo Cliente', 'Segundo Cliente')
        if result:
            emailSet.update(result)
        
        result = processEmailSecundaryClient(row, 'CPF Terceiro Cliente', 'Terceiro Cliente')
        if result:
            emailSet.update(result)
        
        result = processEmailSecundaryClient(row, 'CPF Quarto Cliente', 'Quarto Cliente')
        if result:
            emailSet.update(result)
        
        result = processEmailSecundaryClient(row, 'CPF Quinto Cliente', 'Quinta Cliente')
        if result:
            emailSet.update(result)
        
        # Guarantors
        result = processEmailSecundaryClient(row, 'CPF Primeiro Fiador', 'Primeiro Fiador')
        if result:
            emailSet.update(result)
        
        result = processEmailSecundaryClient(row, 'CPF Segundo Fiador', 'Segundo Fiador')
        if result:
            emailSet.update(result)
        
        result = processEmailSecundaryClient(row, 'CPF Terceiro Fiador', 'Terceiro Fiador')
        if result:
            emailSet.update(result)

    """
    Email structure in db
    CREATE TABLE EMAIL
    (
        cpfCnpjCliente VARCHAR(14) FOREIGN KEY REFERENCES Cliente(cpfCnpj) ON DELETE CASCADE,
        email VARCHAR(255) PRIMARY KEY,
        ativo BIT NOT NULL DEFAULT 1
    );
    """
    sqlInsert = """
    INSERT INTO EMAIL (cpfCnpjCliente, email, ativo)
    VALUES (?, ?, 1)
    """
    # Insert all the set data into the database at once
    if len(emailSet) > 0:
        cursor.executemany(sqlInsert, emailSet)
        conn.commit()
    else:
        print("No emails to insert.")
    # Commit transaction
    conn.commit()

    # Close the cursor
    cursor.close()



def processEmailSecundaryClient(row: pd.Series, cpfRowIndex: str, suffixMail: str):
    #check if there is a cpf in the row
    if pd.isna(row[cpfRowIndex]) or row[cpfRowIndex] == "nan" or len(str(row[cpfRowIndex]).strip()) == 0:
        return
    #clean the cpf
    cpfLimpo = cleanCpf(row[cpfRowIndex])
    if not cpfLimpo:
        return
    
    prefix = 'E-mail'
    results = set()

    email = cleanEmail(str(row[f'{prefix} {suffixMail}']))
    # Set to hold unique email entries
    if email:
        results.add((cpfLimpo, email))

    return results
    



