import requests
import pandas as pd

def consultaCep(cep):
    url = f"https://viacep.com.br/ws/{cep}/json/"
    response = requests.get(url)

    if response.status_code == 200:
        dados = response.json()
        if "erro" in dados:
            return "CEP não encontrado."
        return dados
