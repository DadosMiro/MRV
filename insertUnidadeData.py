from MRVDataFeed import assertNotNullAndType
import pyodbc
from datetime import datetime
import pandas as pd

def insertUnidadeData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):
    """
    Insert property unit data
    
    PSEUDOCODE:
    - For each unique combination of (Código do Produto, Bloco, Unidade):
        - Get codigoEmpreendimento from Empreendimento table -> Select empreendimento 
        - Clean and validate Bloco and Unidade values
        - Build descricaoCompleta from Residencial column
        - INSERT INTO Unidade (codigoEmpreendimento, bloco, numero, andar, descricaoCompleta)
        - VALUES (codigo_empreendimento, bloco_limpo, unidade_limpa, NULL, residencial_completo)
    ### 10. Unidade (Unit/Property)
```
+ id: PK INT IDENTITY(1,1)
+ codigoEmpreendimento: FK INT -- References Empreendimento.codigo
+ bloco: VARCHAR(10)
+ descricaoCompleta: VARCHAR(500)
```
    """
    assertNotNullAndType(conn, pyodbc.Connection, "Conexão com o banco de dados")
    assertNotNullAndType(dataChunk, pd.DataFrame, "DataFrame com os dados da unidade")

    cursor = conn.cursor()

    # Select the id code of the empreendimento
    empreendimentoQuery  = """
    SELECT codigoEmpreendimento FROM Empreendimento WHERE nome = ? AND empresa = ? AND cnpjEmpresa = ?
    """

    unidadeSet = set()
    #TRATAR OS DADOS DO DATAFRAME DE PESQUISA(USADO PARA TODO ROW)

    dataChunk['Nome da Empresa'] = dataChunk['Nome da Empresa'].fillna("[Aviso]Dado Inconsistente").astype(str)
    dataChunk['CNPJ da Empresa'] = dataChunk['CNPJ da Empresa'].fillna("").astype(str).str.replace(r'\D', '', regex=True)
    dataChunk['Descrição do Residencial'] = dataChunk['Descrição do Residencial'].fillna("").astype(str)

    for index, row in dataChunk.iterrows():
        codigoEmpreendimento = cursor.execute(empreendimentoQuery, (row['Descrição do Residencial'], row['Nome da Empresa'], row['CNPJ da Empresa'])).fetchone()
        if codigoEmpreendimento:
            #limpar dados da unidade
            bloco = str(row['Bloco']).strip()
            unidade = str(row['Unidade']).strip()
            descricaoCompleta = f"{row['Descrição do Residencial']} - {bloco} {unidade}"

            codigoEmpreendimento[0] = int(codigoEmpreendimento[0])  # Convert to int

            #asserte todos os valores
            assertNotNullAndType(codigoEmpreendimento[0], int, "código do empreendimento")
            assertNotNullAndType(bloco, str, "bloco")
            assertNotNullAndType(unidade, str, "unidade")
            assertNotNullAndType(descricaoCompleta, str, "descrição completa")

            size = len(unidadeSet)
            unidadeSet.add((codigoEmpreendimento[0], bloco, unidade, descricaoCompleta))
            size2 = len(unidadeSet)
            if size == size2:
                continue
            # Inserir os dados únicos de unidade

            unidadeInsertQuery = """            INSERT INTO Unidade (codigoEmpreendimento, bloco, descricaoCompleta)
            VALUES (?, ?,?)"""

            cursor.execute(unidadeInsertQuery, (codigoEmpreendimento[0], bloco, descricaoCompleta))
            
        else:
            raise ValueError(f"Empreendimento não encontrado para {row['Descrição do Residencial']} - {row['Nome da Empresa']} - {row['CNPJ da Empresa']}")
    sqlDropDuplicates = """
    DELETE FROM Unidade 
    WHERE id IN (
        SELECT id FROM (
            SELECT id,
                   ROW_NUMBER() OVER (PARTITION BY codigoEmpreendimento, bloco, descricaoCompleta ORDER BY id) AS rn
            FROM Unidade
        ) AS CTE 
        WHERE rn > 1
    )
    """
    cursor.execute(sqlDropDuplicates)

    conn.commit()
    cursor.close()

    pass