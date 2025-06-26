from MRVDataFeed import assertType
import pyodbc
from datetime import datetime
import pandas as pd
from insertApiCep import consultaCep

"""
    Insert address data from customer address fields
    
    PSEUDOCODE:
    - For each unique address combination:
        - Parse Endereço do Cliente to extract: logradouro, numero, complemento
        - Use regex to split full address into components
        - Clean CEP do Cliente (remove special chars, ensure 8 digits)
        - INSERT INTO Endereco (logradouro, numero, complemento, bairro, cidade, uf, cep, tipoEndereco)
        - VALUES (logradouro_parsed, numero_parsed, complemento_parsed, bairro_cliente, 
                  cidade_cliente, estado_cliente, cep_limpo, 'RESIDENCIAL')
        - Store enderecoId for relationship table
    """
def insertEnderecoData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    assertType(conn, pyodbc.Connection, "Database connection")
    assertType(dataChunk, pd.DataFrame, "Data chunk")

    cursor = conn.cursor()
 
    Endereco = set()
        
    for index, row in dataChunk.iterrows():
        cep = str(row['CEP do Cliente']).replace('.', '').replace('-', '').strip()
        assertType(cep, str, "CEP do Cliente")
        if len(cep) != 8 or not cep.isdigit():
            cep = "N/A"
        logradouro = str(row['Endereço do Cliente']).strip()
        assertType(logradouro, str, "Endereço do Cliente")
        if not logradouro or logradouro == "nan" or pd.isna(logradouro):
            endereco_via_cep = consultaCep(cep)
            if endereco_via_cep and isinstance(endereco_via_cep, dict):
                 logradouro = endereco_via_cep.get("logradouro", "N/A").strip().title()
        else:
            logradouro = "N/A"
        bairro = str(row['Bairro do Cliente']).strip()
        assertType(bairro, str, "Bairro do Cliente")
        if not bairro or bairro == "nan" or pd.isna(bairro):
            bairro_via_cep = bairro_via_cep.get("bairro", "N/A").strip().title() if endereco_via_cep else "N/A"
    