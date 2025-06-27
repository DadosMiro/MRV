from MRVDataFeed import assertType
import pyodbc
from datetime import datetime
import pandas as pd
from helper import cleanCpf, getDigits, convertCurrencyToFloat



def insertContratoData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    # For each combination of unique (contract code && client 1 && client 2 && client 3 && client n, && guarantor 1 && guarantor n)
    assertType(conn, pyodbc.Connection, "Database connection")
    assertType(dataChunk, pd.DataFrame, "Data chunk")

    # Filter dataChunk to remove rows with missing or invalid 'contrato'
    data = dataChunk.copy()
    data = data[data['Contrato'].notna() & (data['Contrato'] != '')]
    
    # Now unique by combination of 'Contrato', 'CPF/CNPJ do Cliente', 'CPF Segundo Cliente', 'CPF Terceiro Cliente', etc.
    # Clean all CPF columns before grouping
    cpfColumns = ['CPF/CNPJ do Cliente', 'CPF Segundo Cliente', 'CPF Terceiro Cliente', 
                   'CPF Quarto Cliente', 'CPF Quinto Cliente', 'CPF Primeiro Fiador', 
                   'CPF Segundo Fiador', 'CPF Terceiro Fiador']
    
    for col in cpfColumns:
        if col in data.columns:
            data[col] = data[col].apply(lambda x: cleanCpf(x) if pd.notna(x) else x)
    
    data = data.groupby(['Contrato', 'CPF/CNPJ do Cliente', 'CPF Segundo Cliente',
                                   'CPF Terceiro Cliente', 'CPF Quarto Cliente', 'CPF Quinto Cliente',
                                   'CPF Primeiro Fiador', 'CPF Segundo Fiador', 'CPF Terceiro Fiador'], as_index=False)
    
    # For each unique group, get the first row and process it
    for _, row in data:
        row = row.iloc[0]
    
        contractNumber = getDigits(str(row['Contrato']))
        if contractNumber == '':
            continue 

        # To find the enterprise id, we'll use the enterprise values
        enterpriseName = str(row['Descrição do Residencial'])
        assertType(enterpriseName, str, "enterprise name")

        companyName = str(row['Nome da Empresa'])
        assertType(companyName, str, "company name")
        if len(companyName) == 0 or companyName == "nan" or pd.isna(companyName):
            companyName = "[Warning]Inconsistent Data"

        companyCnpj = str(row['CNPJ da Empresa']).replace('.', '').replace('/', '').replace('-', '')
        assertType(companyCnpj, str, "company CNPJ")
        if len(companyCnpj) != 14:
            companyCnpj = None

        # Select the id code of the enterprise
        enterpriseQuery = """SELECT codigoEmpreendimento FROM Empreendimento WHERE nome = ? AND empresa = ? AND cnpjEmpresa = ?"""
        # Execute the query
        cursor = conn.cursor()
        cursor.execute(enterpriseQuery, (enterpriseName, companyName, companyCnpj))
        enterpriseCode = cursor.fetchone()
        if not enterpriseCode:
            raise ValueError(f"Enterprise not found for {enterpriseName} - {companyName} - {companyCnpj}")
        
        enterpriseCode = int(enterpriseCode[0])  # Convert to int
        assertType(enterpriseCode, int, "enterprise code")

        productName = str(row['Descrição do Residencial'])
        assertType(productName, str, "product name")

        selectSubOperation = """SELECT id FROM SubOperacao WHERE codigoEmpresa = ? AND nomeEmpresaFilial = ? AND nomeProduto = ? AND cnpjEmpresa = ?"""
        cursor.execute(selectSubOperation, (row['Código da Empresa'], row['Nome da Empresa'], productName, companyCnpj))

        subOperationId = cursor.fetchone()
        if not subOperationId:
            raise ValueError(f"SubOperation not found for {row['Código da Empresa']} - {row['Nome da Empresa']} - {productName} - {companyCnpj}")

        subOperationId = int(subOperationId[0])  # Convert to int
        assertType(subOperationId, int, "sub operation ID")
        
        # Clean and validate contract number
        # Clean and validate CPF/CNPJ of main client
        debtorCpfCnpj = cleanCpf(row['CPF/CNPJ do Cliente'])
        if not debtorCpfCnpj:
            continue
        assertType(debtorCpfCnpj, str, "debtor CPF/CNPJ")

        # Contract date is dd/mm/yyyy converting to date
        contractDate = pd.to_datetime(row['Data do Contrato'], format='%d/%m/%Y', errors='coerce')
        if pd.isna(contractDate):
            raise ValueError(f"Invalid contract date: {row['Data do Contrato']}")
        assertType(contractDate, datetime, "contract date")

        # Contract value at sale
        saleValue = convertCurrencyToFloat(row['Valor da Venda'])
        assertType(saleValue, float, "contract value at sale")

        # Bank signature date
        signatureDate = pd.to_datetime(row['Data Real da Assinatura no Banco'], format='%d/%m/%Y', errors='coerce')
        if pd.isna(signatureDate):
            signatureDate = None
        
        # Key delivery date
        keyDeliveryDate = pd.to_datetime(row['Data Real de Entrega de Chaves'], format='%d/%m/%Y', errors='coerce')

        if pd.isna(keyDeliveryDate):
            keyDeliveryDate = None
        
        # Financing type
        financingType = str(row['Plano de Venda'])
        # Map Plano de Venda to standard financing type
        financingTypeKey = financingType.strip().upper()
        financingTypeMap = {
            'ASSOCIATIVO': 'FINANCIAMENTO GERAL ASSOCIATIVO',
            'BANCÁRIO CORRIGIDO': 'FINANCIAMENTO GERAL CORRIGIDO',
            'BANCÁRIO FIXO': 'FINANCIAMENTO GERAL FIXO',
            'BANCO DO BRASIL ASSOCIATIVO': 'FINANCIAMENTO BB ASSOCIATIVO',
            'BANCO DO BRASIL ASSO': 'FINANCIAMENTO BB ASSOCIATIVO',
            'CEF ASSOCIATIVO': 'FINANCIAMENTO CEF ASSOCIATIVO',
            'IGP-M +': 'FINANCIAMENTO GERAL IGP-M',
            'CEF ASSOCIATIVO IGP-M +': 'FINANCIAMENTO CEF ASSOCIATIVO IGP-M',
            'CEF ASSOCIATIVO IGP-M + 1%': 'FINANCIAMENTO CEF ASSOCIATIVO IGP-M',
            'CEF ASSOCIATIVO CORRIGIDO': 'FINANCIAMENTO CEF ASSOCIATIVO CORRIGIDO',
            'CEF ASSOCIATIVO P': 'FINANCIAMENTO CEF ASSOCIATIVO',
            'IPCA +1%': 'FINANCIAMENTO GERAL IPCA',
            'CEF ASSOCIATIVO P IPCA +1%': 'FINANCIAMENTO CEF ASSOCIATIVO IPCA',
            'CEF ASSOCIATIVO P IP': 'FINANCIAMENTO CEF ASSOCIATIVO IP',
            'FLEX À VISTA': 'FINANCIAMENTO GERAL FLEX',
            'FINANCIAMENTO BANCÁRIO': 'FINANCIAMENTO GERAL BANCÁRIO'
        }
        financingType = financingTypeMap.get(financingTypeKey, financingTypeKey)

        assertType(financingType, str, "financing type")

        collectionType = str(row['Perfil']).strip().upper()
        assertType(collectionType, str, "collection type")

        contractPriority = 0  # By default, no classification
        locator = 0  # By default, no locator

        # Occurrence
        occurrence = str(row['Ocorrências']).strip().upper()
        assertType(occurrence, str, "occurrence")

        preventiveOccurrences = str(row['Ocorrências Impeditivas']).strip().upper()
        assertType(preventiveOccurrences, str, "preventive occurrences")
        
        # Observations as empty strings
        observations = ''

        # Insert into Contract table
        sqlInsert = """
        INSERT INTO Contrato (numero, codigoEmpreendimento, subOperacaoId, cpfCnpjDevedor, valorNaVenda, 
                              dataContrato, dataAssinatura, dataEntregaChaves, tipoDeFinanciamento, 
                              ajuizado, tipoCobranca, prioridadeContrato, localizador, ocorrencia, 
                              impeditivoCobranca, observacoes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(sqlInsert, (contractNumber, enterpriseCode, subOperationId, debtorCpfCnpj, saleValue,
                                   contractDate, signatureDate, keyDeliveryDate, financingType,
                                   0, collectionType, contractPriority, locator, occurrence,
                                   preventiveOccurrences, observations))
        # Commit the transaction
        conn.commit()
"""
11. Contrato (Contract)
+ numero: PK VARCHAR(50) NOT NULL
+ codigoEmpreendimento: FK VARCHAR(20) -- References Empreendimento.codigo
+ subOperacaoId: FK INT -- References SubOperacao.id
+ cpfCnpjDevedor: FK VARCHAR(14) NOT NULL -- References Cliente.cpfCnpj
+ valorNaVenda: DECIMAL(19,4) NOT NULL
+ dataContrato: DATE NOT NULL
+ dataAssinatura: DATE
+ dataEntregaChaves: DATE
+ tipoDeFinanciamento: VARCHAR(20) NOT NULL -- 'ASSOCIATIVO', 'FINANCIAMENTO'
+ ajuizado: BIT DEFAULT 0
+ tipoCobranca: VARCHAR(20) -- 'EXTRAJUDICIAL', 'JUDICIAL'
+ prioridadeContrato: TINYINT CHECK (prioridadeContrato <= 10)-- no classification, just is equivalent to 0
+ localizador: BIT DEFAULT 0
+ ocorrencia:  VARCHAR(100)
+ impeditivoCobranca: VARCHAR(100)
+ observacoes: VARCHAR(1000)

### 12. ContratoCliente (Contract-Customer Relationship)
```
+ id: PK INT IDENTITY(1,1)
+ numeroContrato: FK VARCHAR(50) NOT NULL -- References Contrato.numero
+ cpfCnpjCliente: FK VARCHAR(14) NOT NULL -- References Cliente.cpfCnpj
+ tipoVinculo: VARCHAR(20) NOT NULL -- 'PRINCIPAL', 'SECUNDARIO', 'TERCEIRO', 'QUARTO', 'QUINTO', 'AVALISTA'
+ ativo: BIT DEFAULT 1
+ dataVinculacao: DATETIME2 DEFAULT GETDATE()

```

def insertContratoClienteData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):

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

    pass
"""