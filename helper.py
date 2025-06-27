def cleanTelefoneData(tel: str):
    """
    Clean phone number by removing special characters and returning only digits.
    """
    return ''.join(filter(str.isdigit, tel))



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
    if len(cepN) < 8 and len(cepN) >= 3:
        #fill with zero on the left, bc stupid people use cpf as number
        cepN = cepN.zfill(8)
    return cepN
    