from concurrent.futures import ThreadPoolExecutor
import pyodbc
import pandas as pd
import sys
import numpy as np

def assertNotNullAndType(obj, expected_type, name):
    """Verifica se o objeto não é nulo e é do tipo esperado."""
    if obj is None:
        raise ValueError(f"[ERROR] {name} cannot be None")
    if not isinstance(obj, expected_type):
        raise TypeError(f"[ERROR] {name} must be of type {expected_type.__name__}, got {type(obj).__name__}")

def connectToDB(dbName : str, userName: str, password: str):
    assertNotNullAndType(dbName, str, "dbName")
    assertNotNullAndType(userName, str, "userName")
    assertNotNullAndType(password, str, "password")

    connection_string = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=192.168.0.11;'
        f'DATABASE={dbName};'
        f'UID={userName};'
        f'PWD={password};'
    )
    return pyodbc.connect(connection_string)


def insertDataTo(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    # This function will be used to insert data into the database
    
        assertNotNullAndType(conn, pyodbc.Connection, "conn")
        assertNotNullAndType(dataChunk, pd.DataFrame, "dataChunk")
        cursor = conn.cursor()
        
        # Begin transaction for data integrity
        try:
            cursor.fast_executemany = True
            insert_query = """
            INSERT INTO [dbo].[CARGA_MRV] ([Origem], [Sistema], [Contrato], [Residencial], [Código do Produto], [Descrição do Residencial], [Bloco], [Unidade], [Número do Cliente], [Nome do Cliente], [CPF/CNPJ do Cliente], [E-mail do Cliente], [Endereço do Cliente], [Bairro do Cliente], [Cidade do Cliente], [Estado do Cliente], [CEP do Cliente], [Telefone Fixo], [Telefone Celular], [Telefone Fax], [Plano de Venda], [Percentual Pago], [Regional], [Data do Contrato], [Valor da Venda], [Código da Divisão], [Código da Empresa], [Nome da Empresa], [CNPJ da Empresa], [Data de Prev. da Assinatura no Banco], [Data Real da Assinatura no Banco], [Data de Prev. de Averbação do Habite-se], [Data Real de Averbação do Habite-se], [Data de Previsão de Entrega de Chaves], [Data Real de Entrega de Chaves], [Bloqueio - Desc do Status da Parcela], [Identificador da Parcela], [Tipo da Parcela], [Descrição da Parcela], [Status da Parcela (SAP)], [Data de Vencimento da Parcela], [Qtd. de dias de Atraso da Parcela], [Faixa de Atraso da Parcela], [Qtd. de dias de Atraso do Contrato], [Faixa de Atraso do Contrato], [Valor Original da Parcela], [Valor do Reajuste da Parcela], [Valor da Parcela Reajustada], [Ocorrências], [Ocorrências Impeditivas], [(Nenhum nome de coluna)], [Telefone 1], [Telefone 2], [Telefone 3], [Telefone 4], [Telefone 5], [Telefone 6], [Telefone 7], [Telefone 8], [Telefone 9], [Telefone 10], [Espaço em branco], [Espaço em branco2], [estrategia02], [estrategia03], [estrategia04], [estrategia05], [Segundo Cliente], [CPF Segundo Cliente], [E-mail Segundo Cliente], [Telefone Segundo Cliente], [Celular Segundo Cliente], [Telefone Comercial Segundo Cliente], [Terceiro Cliente], [CPF Terceiro Cliente], [E-mail Terceiro Cliente], [Telefone Terceiro Cliente], [Celular Terceiro Cliente], [Telefone Comercial Terceiro Cliente], [Quarto Cliente], [CPF Quarto Cliente], [E-mail Quarto Cliente], [Telefone Quarto Cliente], [Celular Quarto Cliente], [Telefone Comercial Quarto Cliente], [Quinta Cliente], [CPF Quinto Cliente], [E-mail Quinto Cliente], [Telefone Quinto Cliente], [Celular Quinto Cliente], [Telefone Comercial Quinto Cliente], [Primeiro Fiador], [CPF Primeiro Fiador], [E-mail Primeiro Fiador], [Telefone Primeiro Fiador], [Celular Primeiro Fiador], [Telefone Comercial Primeiro Fiador], [Segundo Fiador], [CPF Segundo Fiador], [E-mail Segundo Fiador], [Telefone Segundo Fiador], [Celular Segundo Fiador], [Telefone Comercial Segundo Fiador], [Terceiro Fiador], [CPF Terceiro Fiador], [E-mail Terceiro Fiador], [Telefone Terceiro Fiador], [Celular Terceiro Fiador], [Telefone Comercial Terceiro Fiador], [Escritorio], [GRUPO DE COBRANÇA], [DESCONTO ESPECIAL], [VALOR DO ÚLTIMO BOLETO], [ÚLTIMO BOLETO CRIADO], [STATUS DO ÚLTIMO BOLETO], [DATA DE VENCIMENTO - ÚLTIMO BOLETO], [DATA DE PAGAMENTO - ÚLTIMO BOLETO], [POSSUI PENDENCIAS DE ADITIVO], [Data do corte para cálculo da multa], [AGE], [Valor da Multa], [Valor da Mora], [Valor atualizado da parcela + multa + mora], [OC_RENEG], [VLRABTPCL], [Categoria meritocracia], [Faixa pontuacao bonus meritocracia], [Perfil], [CRI], [DataAdicionado])
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            cursor.executemany(insert_query, [tuple(row) for row in dataChunk.values])
            conn.commit()
        except Exception as e:
            print(f"Error inserting data: {e}")
            conn.rollback()
        finally:
            cursor.close()

def cleanDataForSqlAndAddCurrentDate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and prepare data for SQL insertion by casting all values to strings
    and handling NULL values.
    
    Args:
        df: DataFrame to clean
        
    Returns:
        Cleaned DataFrame with all values as strings ready for SQL insertion
    """
    # Create a copy to avoid modifying the original DataFrame
    cleaned_df = df.copy()
    
    # Replace infinity and NaN values with None (which becomes NULL in SQL)
    cleaned_df = cleaned_df.replace([np.inf, -np.inf], np.nan)
    
    # Convert all columns to string type
    for col in cleaned_df.columns:
        cleaned_df[col] = cleaned_df[col].astype(str)
        
        # Replace 'nan' strings with None
        cleaned_df[col] = cleaned_df[col].replace('nan', None)
        
        # Truncate any strings that are too long for SQL Server
        max_length = 8000
        cleaned_df[col] = cleaned_df[col].apply(
            lambda x: x[:max_length] if isinstance(x, str) and len(x) > max_length else x
        )
    
    # Convert None to NULL for SQL
    cleaned_df = cleaned_df.where(pd.notnull(cleaned_df), None)
    #Add current date as last column
    cleaned_df['DataAdicionado'] = pd.to_datetime('now').strftime('%Y-%m-%d')
    

    return cleaned_df
def print_progress_bar(processed_rows, total_rows, batch_num):
    """Print a progress bar that clears the previous line."""
    #clear the previous line
    if processed_rows == 0:
        print("\rProcessing batches...", end='', flush=True)
    else:
        print("\r", end='', flush=True)
        
    progress_percent = (processed_rows / total_rows) * 100
    filled_bars = int(progress_percent // 10)
    progress_bar = '[' + '#' * filled_bars + ' ' * (10 - filled_bars) + ']'
    print(f"\rProcessed batch {batch_num}: {processed_rows}/{total_rows} rows {progress_bar} {progress_percent:.1f}%", end='', flush=True)
    

    
def main():
    BATCH_SIZE = 4500  # Maximum rows per batch

    # Check if the script is run with the correct number of arguments
    if len(sys.argv) < 2:
        raise ValueError("Usage: MRVDataFeed.py [PATH]. (PATH is the path to the CSV file)")
    
    #make sure the path leads to a valid CSV file
    assertNotNullAndType(sys.argv[1], str, "CSV file path")
    if not sys.argv[1].endswith('.csv'):
        raise ValueError("The provided path must point to a CSV file.")
    
    try:
        conn = connectToDB("TesteDB", "arthuroliveira", "12345")
        print("Connection to database established successfully.")
    except Exception as e:
        print(f"Failed to connect to database: {str(e)}")
        return
    
    try:
        #for problematic lines categorize as null
        mrvData = pd.read_csv(sys.argv[1], sep= ';',encoding='utf-8', low_memory=False)
        print("CSV file loaded successfully.")
        #print csv dimensions
        print(f"CSV file dimensions: {mrvData.shape[0]} rows, {mrvData.shape[1]} columns")
    except Exception as e:
        print(f"Failed to load CSV file: {str(e)}")
        return
    
    mrvData = cleanDataForSqlAndAddCurrentDate(mrvData)
    
    # Process data in batches
    total_rows = len(mrvData)
    processed_rows = 0
    
    for i in range(0, total_rows, BATCH_SIZE):
        batch = mrvData.iloc[i:i+BATCH_SIZE]
        batch_size = len(batch)
        
        try:
            insertDataTo(conn, batch)
            processed_rows += batch_size
            
            # Calculate progress percentage and create progress bar
            print_progress_bar(processed_rows, total_rows, i//BATCH_SIZE + 1)
        except Exception as e:
            print(f"Error processing batch {i//BATCH_SIZE + 1}: {e}")
    
    print("Data insertion completed successfully.")
    conn.close()


if __name__ == "__main__":
    main()