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
    Se isso for feito será necessário alterar o app atual de insercao de dados.
    """
    # Consulta SQL de inserção
    sqlInsert = """
    INSERT INTO Operacao (nomeOperacao, gestorResponsavel, periodoAtualizacao, ativa, dataInicio, dataFim)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    # Prepara os dados para inserção
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
