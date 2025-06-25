from MRVDataFeed import assertNotNullAndType
import pyodbc
from datetime import datetime

def insertOperacaoData(conn: pyodbc.Connection):
    assertNotNullAndType(conn, pyodbc.Connection, "Database connection")
    """
    Insere dados na tabela Operacao como fallback de insertSubOperation.
    Valores fixos:
      - nomeOperacao: "MRV"
      - gestorResponsavel: "Joao Paulo"
      - periodoAtualizacao: 30 (dias)
      - ativa: 1
      - dataInicio: datetime.now()
      - dataFim: NULL
    No futuro podemos fazer como argumentos do arquivo python e passar via CLI.
    Se isso for feito sera necessario alterar o app atual de insercao de dados.
    """
    # Sql Query Insert

    sqlInsert = """
    INSERT INTO Operacao (nomeOperacao, gestorResponsavel, periodoAtualizacao, ativa, dataInicio, dataFim)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    # Prepare the data for insertion
    data = {
        "nomeOperacao": "MRV",
        "gestorResponsavel": "Joao Paulo",
        "periodoAtualizacao": 30,
        "ativa": 1,
        "dataInicio": datetime.now(),
        "dataFim": None
    }

    cursor = conn.cursor()
    cursor.execute(sqlInsert, list(data.values()))
    conn.commit()
    cursor.close()