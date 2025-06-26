def cleanTelefoneData(tel: str):
    """
    Clean phone number by removing special characters and returning only digits.
    """
    return ''.join(filter(str.isdigit, tel))



def cleanCpf(cpf: str):
    """
    Clean CPF/CNPJ by removing special characters and returning only digits.
    """
    return ''.join(filter(str.isdigit, cpf))

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