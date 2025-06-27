import pandas as pd
import requests
import time

def cleanTelefoneData(tel: str):
    """
    Clean phone number by removing special characters and returning only digits.
    """
    return ''.join(filter(str.isdigit, tel)) if ''.join(filter(str.isdigit, tel)) != '' else None 



def cleanCpf(cpf: str):
    """
    Clean CPF/CNPJ by removing special characters and returning only digits.
    """
    cpfN = ''.join(filter(str.isdigit, str(cpf)))
    if len(cpfN) > 11:
        return None
    if len(cpfN) < 11 and len(cpfN) >= 3:
        #fill with zero on the left, bc stupid people use cpf as number
        cpfN = cpfN.zfill(11)
    
    return cpfN

def getDigits(s: str):
    """
    Extract digits from a string.
    Returns a string of digits or None if no digits are found.
    """
    digits = ''.join(filter(str.isdigit, s))
    return digits #for when digits are not found, it will return an empty string(if it needs just change it to return None)
def cleanEmail(email: str):
    """
    Clean email by checking format and removing leading/trailing spaces.
    Returns None if the email is invalid.
    """
    if isinstance(email, str) and email.count('@') == 1:
        local, domain = email.split('@')
        if local and domain and '.' in domain:
            return email.strip().lower()
    return None


def cleanCep(cep: str):
    """
    Clean Cep by removing special characters and returning only digits.
    """
    cepN = ''.join(filter(str.isdigit, str(cep)))
    if len(cepN) > 8:
        return None
    if len(cepN) < 8 and len(cepN) >= 6:
        cepN = cepN.zfill(8)
    return cepN

def convertCurrencyToFloat(value):
    # treat missing or 'NÃO INFORMADO' as zero
    if pd.isna(value) or value == '' or value == 'NÃO INFORMADO':
        return 0.0
    # strip 'R$', spaces and switch comma to dot
    cleanedCurrencyValue = str(value).replace('R$', '').replace(',', '.').replace(' ', '')
    if cleanedCurrencyValue.count('.') > 1:
        # only the last dot is the decimal separator
        cleanedCurrencyValue = f"{cleanedCurrencyValue.rsplit('.',1)[0].replace('.', '')}.{cleanedCurrencyValue.rsplit('.',1)[1]}" 
    return float(cleanedCurrencyValue)


def consultaCepApi(cep):
    time.sleep(0.3)
    url = f"https://viacep.com.br/ws/{cep}/json/"
    response = requests.get(url) 

    if response.status_code == 200:
        dados = response.json()
        if "erro" in dados:
            return "CEP não encontrado."   
        return dados
    else: 
        return f"Erro na consulta" 
 

