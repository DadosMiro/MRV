from MRVDataFeed import assertType
import pyodbc
from datetime import datetime
import pandas as pd

def insertParcelaData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    """
    Insert installment data from CSV parcela fields
    
    get unique contratoCodes
    for each unique contratoCode:
    - get data from parcela in the csv
    - treat data from parcela
    - insert into Parcela table with the following fields:
        - contratoCode
        - numeroParcela
        - valorParcela
        - dataVencimento
        - dataPagamento
        - statusPagamento
### 13. Parcela (Installment)
**Purpose**: Payment installments

| Database Column | CSV Column | Transformation Notes |
|---|---|---|
| numeroContrato | Contrato | Foreign key reference |
| codigoParcela | Identificador da Parcela | Direct mapping |
| tipoParcela | Tipo da Parcela | Map 'E'→'ENTRADA', 'R'→'REGISTRO', etc. |
| descricaoParcela | Descrição da Parcela | Direct mapping |
| valorOriginal | Valor Original da Parcela | Parse currency format |
| valorAtualizado | Valor da Parcela Reajustada | Parse currency format |
| valorPendente | Valor atualizado da parcela + multa + mora | Parse currency format |
| dataVencimento | Data de Vencimento da Parcela | Parse date format |
| statusParcela | Status da Parcela (SAP) | Map 'AT'→'VENCIDO', 'PB'→'EM_ABERTO' |
| diasAtraso | Qtd. de dias de Atraso da Parcela | Direct mapping |
| faixaAtraso | Faixa de Atraso da Parcela | Direct mapping |
| planoVenda | Plano de Venda | Direct mapping |
    remove duplicates from Parcela table
    """
    assertType(conn, pyodbc.Connection, "Database connection")
    assertType(dataChunk, pd.DataFrame, "DataFrame with installment data")
    
    # Select unique contratoCodes
    contratoCodes = dataChunk['Contrato'].unique()
    
    processed_data = []

    for contratoCode in contratoCodes:
        # Get data for this contract
        contract_data = dataChunk[dataChunk['Contrato'] == contratoCode]
        
        for _, row in contract_data.iterrows():
            # Data treatment
            processed_row = {
                'numeroContrato': row['Contrato'],
                'codigoParcela': row['Identificador da Parcela'],
                'tipoParcela': row['Tipo da Parcela'].replace('E', 'ENTRADA').replace('R', 'REGISTRO') if pd.notna(row['Tipo da Parcela']) else None,
                'descricaoParcela': row['Descrição da Parcela'] if pd.notna(row['Descrição da Parcela']) else None,
                'valorOriginal': float(str(row['Valor Original da Parcela']).replace(',', '.').replace('R$', '').strip()) if pd.notna(row['Valor Original da Parcela']) else None,
                'valorAtualizado': float(str(row['Valor da Parcela Reajustada']).replace(',', '.').replace('R$', '').strip()) if pd.notna(row['Valor da Parcela Reajustada']) else None,
                'valorPendente': float(str(row['Valor atualizado da parcela + multa + mora']).replace(',', '.').replace('R$', '').strip()) if pd.notna(row['Valor atualizado da parcela + multa + mora']) else None,
                'dataVencimento': pd.to_datetime(row['Data de Vencimento da Parcela'], errors='coerce') if pd.notna(row['Data de Vencimento da Parcela']) else None,
                'statusParcela': row['Status da Parcela (SAP)'].replace('AT', 'VENCIDO').replace('PB', 'EM_ABERTO') if pd.notna(row['Status da Parcela (SAP)']) else None,
                'diasAtraso': int(row['Qtd. de dias de Atraso da Parcela']) if pd.notna(row['Qtd. de dias de Atraso da Parcela']) else None,
                'faixaAtraso': row['Faixa de Atraso da Parcela'] if pd.notna(row['Faixa de Atraso da Parcela']) else None,
                'planoVenda': row['Plano de Venda'] if pd.notna(row['Plano de Venda']) else None
            }
            processed_data.append(processed_row)
    
    # Remove duplicates
    processed_df = pd.DataFrame(processed_data).drop_duplicates()
    
    # Insert into Parcela table
    