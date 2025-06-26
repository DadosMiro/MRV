from concurrent.futures import ThreadPoolExecutor
import pyodbc
import pandas as pd
import sys
import numpy as np
import re
from datetime import datetime

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

"""
NEW INSERTION FUNCTIONS FOR NORMALIZED DATABASE TABLES
Based on CSV-to-Database mapping documentation
"""
#Done
def insertOperacaoData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    """
    Insert operation data from CSV Sistema column
    
    PSEUDOCODE:
    - Extract unique Sistema values from dataChunk
    - For each unique sistema:
        - Clean and validate sistema name
        - Check if operacao already exists in database
        - If not exists:
            - INSERT INTO Operacao (nomeOperacao, gestorResponsavel, ativa)
            - VALUES (sistema, NULL, 1)
        - Store operacao ID for foreign key references
    """
    pass
#Done
def insertSubOperacaoData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    """
    Insert sub-operation data linking to operations
    
    PSEUDOCODE:
    - For each unique combination of (Sistema, Código da Empresa, Nome da Empresa):
        - Get operacaoId from previously inserted Operacao
        - Clean CNPJ da Empresa (remove special chars)
        - INSERT INTO SubOperacao (operacaoId, codigoEmpresa, nomeEmpresaFilial, 
                                   nomeProduto, cnpjEmpresa, nomeEmpresa)
        - VALUES (operacaoId, codigo_empresa, nome_empresa, descricao_residencial, 
                  cnpj_limpo, nome_empresa)
    """
    pass
#Done
def insertEmpreendimentoData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    """
    Insert development/project data from CSV
    
    PSEUDOCODE:
    - For each unique Código do Produto:
        - Extract and clean: codigo, nome, cidade, uf, empresa, cnpj
        - Map Código do Produto -> codigo
        - Map Descrição do Residencial -> nome
        - Derive cidade and uf from regional/customer data
        - Clean CNPJ da Empresa (remove special chars)
        - INSERT INTO Empreendimento (codigo, nome, cidade, uf, empresa, cnpjEmpresa, ativo)
        - VALUES (codigo_produto, descricao_residencial, cidade_derivada, uf_derivada, 
                  nome_empresa, cnpj_limpo, 1)
    """
    pass
#Done
def insertUnidadeData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    """
    Insert property unit data
    
    PSEUDOCODE:
    - For each unique combination of (Código do Produto, Bloco, Unidade):
        - Get codigoEmpreendimento from Empreendimento table
        - Clean and validate Bloco and Unidade values
        - Build descricaoCompleta from Residencial column
        - INSERT INTO Unidade (codigoEmpreendimento, bloco, numero, andar, descricaoCompleta)
        - VALUES (codigo_empreendimento, bloco_limpo, unidade_limpa, NULL, residencial_completo)
    """
    pass
#Done
def insertClienteData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    """
    Insert customer data from all customer columns in CSV
    
    PSEUDOCODE:
    - Define customer columns: [CPF/CNPJ do Cliente, CPF Segundo Cliente, CPF Terceiro Cliente, 
                               CPF Quarto Cliente, CPF Quinto Cliente, CPF Primeiro Fiador, etc.]
    - For each customer column:
        - Clean CPF/CNPJ (remove special chars, pad CPF with zeros)
        - Validate CPF/CNPJ using algorithms
        - Determine tipoCliente: len(11) -> 'FISICO', len(14) -> 'JURIDICO'
        - Get corresponding nome from Nome columns
        - Get corresponding email from E-mail columns (handle "NÃO INFORMADO" as NULL)
        - Check if customer already exists (avoid duplicates)
        - INSERT INTO Cliente (cpfCnpj, nome, email, ativo, dataInclusao, tipoCliente)
        - VALUES (cpf_cnpj_limpo, nome_cliente, email_tratado, 1, GETDATE(), tipo_cliente)
    """
    pass

def insertInfoFisicaData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    """
    Insert individual customer details
    
    PSEUDOCODE:
    - For each customer with tipoCliente = 'FISICO':
        - Get cpfCnpj from Cliente table
        - INSERT INTO InfoFisica (cpfCnpj, cnh, passaporte, tituloEleitor, salario, 
                                  estadoCivil, profissao)
        - VALUES (cpf_cliente, NULL, NULL, NULL, NULL, NULL, NULL)
        - Note: Most fields are NULL as they're not available in CSV
    """
    pass

def insertInfoJuridicaData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    """
    Insert corporate customer details
    
    PSEUDOCODE:
    - For each customer with tipoCliente = 'JURIDICO':
        - Get cpfCnpj from Cliente table
        - Extract cnpjRaiz (first 8 digits of CNPJ)
        - Get razaoSocial from corresponding Nome da Empresa column
        - INSERT INTO InfoJuridica (cpfCnpj, inscricaoEstadual, cnpjRaiz, razaoSocial, 
                                    cnpjGrupoEconomico, segmento, ramoAtividade, faturamento)
        - VALUES (cnpj_cliente, NULL, cnpj_raiz, razao_social, NULL, NULL, NULL, NULL)
    """
    pass
#In progress
def insertEnderecoData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
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
    pass
#In progress
def insertClienteEnderecoData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    """
    Insert customer-address relationships
    
    PSEUDOCODE:
    - For each customer-address combination:
        - Get cpfCnpjCliente from Cliente table
        - Get enderecoId from Endereco table
        - INSERT INTO ClienteEndereco (cpfCnpjCliente, enderecoId, principal, ativo, dataVinculacao)
        - VALUES (cpf_cnpj_cliente, endereco_id, 1, 1, GETDATE())
        - Set principal = 1 for first address, 0 for additional addresses
    """
    pass

def insertTelefoneData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    """
    Insert phone number data from multiple phone columns
    
    PSEUDOCODE:
    - Define phone column mappings:
        - Telefone Fixo -> 'RESIDENCIAL'
        - Telefone Celular -> 'CELULAR'  
        - Telefone Fax -> 'COMERCIAL'
        - Telefone 1-10 -> Additional phone entries
        - Secondary customer phone columns -> respective customers
    - For each phone column and customer combination:
        - Clean phone number (remove special chars, standardize format)
        - Determine tipoTelefone based on column name
        - Set principal = 1 for first phone, 0 for others
        - INSERT INTO Telefone (cpfCnpjCliente, numero, tipoTelefone, principal)
        - VALUES (cpf_cnpj_cliente, numero_limpo, tipo_telefone, principal_flag)
    """
    pass

def insertContratoData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    """
    Insert contract data from CSV contract fields
    
    PSEUDOCODE:
    - For each contract row:
        - Get codigoEmpreendimento from Empreendimento table using Código do Produto
        - Clean and validate Contrato number
        - Parse currency values (remove R$, commas, convert to decimal)
        - Parse dates (convert DD/MM/YYYY to YYYY-MM-DD)
        - Map status codes: 'AT' -> status mapping
        - Map Plano de Venda to tipoContrato
        - INSERT INTO Contrato (numero, codigoEmpreendimento, cpfCnpjDevedor, valorContrato, 
                                dataContrato, dataAssinatura, dataEntregaChaves, dataEntregaPrevista,
                                dataLiberacaoFinanciamento, tipoContrato, statusContrato, origemDivida,
                                gestorResponsavel, diasAtraso, faixaAtraso, tipoCobranca, observacoes)
        - VALUES (numero_contrato, codigo_empreendimento, cpf_cnpj_devedor, valor_limpo, 
                  data_contrato_parsed, data_assinatura_parsed, data_entrega_parsed, 
                  data_prevista_parsed, data_liberacao_parsed, tipo_contrato_mapped, 
                  status_contrato_mapped, origem, NULL, dias_atraso, faixa_atraso, 
                  grupo_cobranca, ocorrencias)
    """
    pass

def insertContratoClienteData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    """
    Insert contract-customer relationships
    
    PSEUDOCODE:
    - For each contract and all associated customers:
        - Main customer (CPF/CNPJ do Cliente) -> tipoVinculo = 'PRINCIPAL'
        - Segundo Cliente -> tipoVinculo = 'SECUNDARIO'
        - Terceiro Cliente -> tipoVinculo = 'TERCEIRO'
        - Primeiro Fiador -> tipoVinculo = 'AVALISTA'
        - Additional customers -> appropriate tipoVinculo
        - For each customer-contract relationship:
            - INSERT INTO ContratoCliente (numeroContrato, cpfCnpjCliente, tipoVinculo, ativo)
            - VALUES (numero_contrato, cpf_cnpj_cliente, tipo_vinculo, 1)
    """
    pass

def insertParcelaData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    """
    Insert installment data from CSV parcela fields
    
    PSEUDOCODE:
    - For each installment row:
        - Get numeroContrato from contract data
        - Clean and parse currency values (remove R$, commas)
        - Parse dates (DD/MM/YYYY to YYYY-MM-DD)
        - Map Tipo da Parcela: 'E' -> 'ENTRADA', 'R' -> 'REGISTRO', etc.
        - Map Status da Parcela: 'AT' -> 'VENCIDO', 'PB' -> 'EM_ABERTO'
        - INSERT INTO Parcela (numeroContrato, codigoParcela, tipoParcela, descricaoParcela,
                               valorOriginal, valorAtualizado, valorPendente, dataVencimento,
                               statusParcela, diasAtraso, faixaAtraso, planoVenda)
        - VALUES (numero_contrato, identificador_parcela, tipo_parcela_mapped, descricao_parcela,
                  valor_original_limpo, valor_reajustado_limpo, valor_pendente_limpo, 
                  data_vencimento_parsed, status_parcela_mapped, dias_atraso_parcela, 
                  faixa_atraso_parcela, plano_venda)
    """
    pass

def insertPagamentoData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    """
    Insert payment data from CSV payment fields
    
    PSEUDOCODE:
    - For each payment record:
        - Get parcelaId from inserted Parcela records
        - Parse VALOR DO ÚLTIMO BOLETO (remove currency formatting)
        - Parse DATA DE PAGAMENTO - ÚLTIMO BOLETO (convert date format)
        - INSERT INTO Pagamento (parcelaId, valorPago, dataPagamento, formaPagamento, numeroDocumento)
        - VALUES (parcela_id, valor_pago_limpo, data_pagamento_parsed, 'BOLETO', NULL)
        - Only insert if payment data exists (not NULL/empty)
    """
    pass

def insertHistoricoCobrancaData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    """
    Insert collection history data
    
    PSEUDOCODE:
    - For each contract with collection history:
        - Parse Ocorrências field to extract action dates and descriptions
        - Map GRUPO DE COBRANÇA to tipoAcao
        - Extract dates from Ocorrências text using regex
        - Get STATUS DO ÚLTIMO BOLETO for statusResultado
        - INSERT INTO HistoricoCobranca (numeroContrato, dataAcao, tipoAcao, descricaoAcao, statusResultado)
        - VALUES (numero_contrato, data_acao_parsed, grupo_cobranca, descricao_ocorrencia, status_boleto)
    """
    pass

def processNormalizedInsertions(conn: pyodbc.Connection, mrvData: pd.DataFrame):
    """
    Execute all normalized table insertions in proper sequence
    
    PSEUDOCODE:
    - Execute insertions in dependency order:
        1. insertOperacaoData(conn, mrvData)
        2. insertSubOperacaoData(conn, mrvData) 
        3. insertEmpreendimentoData(conn, mrvData)
        4. insertUnidadeData(conn, mrvData)
        5. insertClienteData(conn, mrvData)
        6. insertInfoFisicaData(conn, mrvData)
        7. insertInfoJuridicaData(conn, mrvData)
        8. insertEnderecoData(conn, mrvData)
        9. insertClienteEnderecoData(conn, mrvData)
        10. insertTelefoneData(conn, mrvData)
        11. insertContratoData(conn, mrvData)
        12. insertContratoClienteData(conn, mrvData)
        13. insertParcelaData(conn, mrvData)
        14. insertPagamentoData(conn, mrvData)
        15. insertHistoricoCobrancaData(conn, mrvData)
    - Handle transaction rollback on any failure
    - Log progress for each table insertion
    """
    pass

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
    
    # Execute normalized table insertions
    try:
        print("Starting normalized database insertions...")
        processNormalizedInsertions(conn, mrvData)
        print("Normalized insertions completed successfully.")
    except Exception as e:
        print(f"Error during normalized insertions: {e}")
        return
    
    # Process data in batches for legacy CARGA_MRV table
    total_rows = len(mrvData)
    processed_rows = 0
    
    print("Starting legacy CARGA_MRV table insertions...")
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