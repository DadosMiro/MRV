import time
import requests

ceps_cache = {} 

def consultaCepComCache(cep):
    cep = cep.strip().replace('.', '').replace('-', '')
    if len(cep) != 8 or not cep.isdigit():
        return "CEP inválido."
    

    if cep in ceps_cache:
        print(f"Usando cache para CEP {cep}")
        return ceps_cache[cep]

    url = f"https://viacep.com.br/ws/{cep}/json/"
    response = requests.get(url)
    
    if response.status_code == 200:
        dados = response.json()
        if "erro" in dados:
            ceps_cache[cep] = "CEP não encontrado."
        else:
            ceps_cache[cep] = dados
    else:
        ceps_cache[cep] = f"Erro na consulta: {response.status_code}"

    time.sleep(0.3) # Aguardar 300ms para evitar sobrecarga na API
    
    return ceps_cache[cep]
