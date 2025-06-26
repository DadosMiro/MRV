from MRVDataFeed import assertType
import pyodbc
from datetime import datetime
import pandas as pd
def insertParcelaData(conn: pyodbc.Connection, dataChunk: pd.DataFrame):

    assertType(conn, pyodbc.Connection, "Database connection")
    assertType(dataChunk, pd.DataFrame, "DataFrame with installment data")
    cursor = conn.cursor()

    # get all unique contract IDs
    contractIdentifiers = dataChunk['Contrato'].unique()

    for contratoCode in contractIdentifiers:
        # select rows for this contract
        contractEntries = dataChunk[dataChunk['Contrato'] == contratoCode]
        
        for _, row in contractEntries.iterrows():
            initialParcelValue = convertCurrencyToFloat(row['Valor Original da Parcela'])
            currentParcelValue = convertCurrencyToFloat(row['Valor da Parcela Reajustada'])
            pendingValue = convertCurrencyToFloat(row['Valor atualizado da parcela + multa + mora'])
            
            # convert due date string to a date, or None if missing
            dueDate = (
                datetime.strptime(row['Data de Vencimento da Parcela'], '%Y-%m-%d')
                        .date()
                if pd.notna(row['Data de Vencimento da Parcela']) 
                else None
            )
            
            # Map status
            parcelStatusMapping = {'AT': 'PARCELA_EM_ATRASO','Z': 'PARCELA_A_VENCER', 'AB': 'PARCELA_ABERTA','PB': 'PARCELA_PROCESSADA_BAIXADA'}
            parcelStatus = parcelStatusMapping.get(row['Status da Parcela (SAP)'], row['Status da Parcela (SAP)'])


            contractTypeMapping = {'EC': 'Carteira',    'SL': 'Carteira',    'J': 'Taxa - Reembolso',    'TC01': 'Desconto',    'DM': 'Desconto',    'VM': 'Desconto',    'PC': 'Desconto',    'DN': 'Desconto',    'E001': 'Carteira',    'E': 'Carteira',    'ER': 'Carteira',    'M': 'Carteira',    'A': 'Carteira',    'N': 'Carteira',    'B001 e B002': 'Financiamento',    'BK': 'Financiamento',    'B003': 'Financiamento',    'L001': 'Financiamento',    'LK': 'Financiamento',    'P001 ou P002': 'Financiamento',    'PK': 'Financiamento',    'P003': 'Financiamento',    'X001': 'Financiamento',    'XK': 'Financiamento',    'K001': 'Kit',    'K': 'Kit',    'KC': 'Kit',    'H': 'Financiamento',    'NH': 'Carteira',    'M500': 'Carteira',    'TI': 'Desconto',    'IH': 'Desconto',    'FP': 'Desconto',    'E100': 'Carteira',    'E300': 'Carteira',    'DF': 'Carteira',    '@': 'Carteira',    'DF99': 'Carteira',    'E500': 'Carteira',    'E120': 'Carteira',    'B005': 'Financiamento',    'P005': 'Financiamento',    'IN': 'Taxa',    'E900': 'Taxa',    'I': 'Taxa',    'R': 'Taxa',    'RF': 'Taxa',    'RI': 'Taxa',    'RT': 'Taxa',    'RS': 'Taxa',    'RR': 'Taxa',    'RC': 'Taxa',    'RE': 'Taxa',    'AU': 'Taxa',    'U': 'Taxa',    'UC': 'Taxa',    'UD': 'Taxa',    'Y': 'Taxa',    'T': 'Taxa',    'D': 'Carteira',    'C': 'Taxa',    'CD': 'Taxa',    'G': 'Taxa',    'O': 'Taxa',    'S': 'Taxa',    'TE': 'Taxa',    'MP': 'Carteira',    'DK': 'Kit',    'E400': 'Desconto',    'E200': 'Carteira',    'Z': 'Carteira',    'Q': 'Kit',    'F': 'Carteira',    'V': 'Carteira'}
            
            parcelTypeDescriptionMapping = {'EC': 'Parcela vinculada ao processo Financiamento da Carteira MRV pela empresa EMCASH, nesse processo a MRV recebe adiantado o valor financiado ao cliente e o cliente paga o parcelamento diretamente a empresa EMCASH com juros e parcelamento pré-fixado. O Financiamento da Carteira a nomenclatura da parcela será EC01 e o financiamento da Diferença de Financiamento caso houver será EC02.', 'SL': 'Parcela vinculada a Subsídios Locais proporcionados por (Prefeituras/Estados)', 'J': 'A Custa Judicial se refere ao reembolso do custo pago pela MRV, para a distribuição de ação judicial para a recuperação do crédito.', 'TC01': 'Será criada nos casos em que for definido por TAC, desconto na parcela de financiamento. Nesses casos, a MRV irá arcar com o subsídio.', 'DM': 'Será criada para conceder desconto referente a reajuste em caso de congelamento de saldo, concedido por Matriz de Alçada, Autorizações Especiais, Acordo Jurídico', 'VM': 'Será criada para conceder desconto referente a diferença entre o valor de mercado da unidade e o valor do saldo devedor do contrato.', 'PC': 'Será criada para conceder desconto referente as Promoções do CI.', 'DN': 'Será criada para reconhecimento do impacto por congelamento de saldo , da troca de índice , ou da inserção de habite-se , quando a parcela de financiamento estiver baixada e o contrato não possuir reajuste suficiente para criação integral das parcelas DM, TI ou IH.', 'E001': 'Entrada do Contrato de Compra e Venda', 'E': 'Entrada', 'ER': 'Parcela de Entrada da Negociação da Urbamais', 'M': 'Mensal', 'A': 'Anual', 'N': 'Intermediária Fixa', 'B001 e B002': 'Parcela de Financiamento', 'BK': 'Parcela de Financiamento', 'B003': 'Diferença de Financiamento', 'L001': 'Parcela de FGTS', 'LK': 'Parcela de FGTS', 'P001 ou P002': 'Parcela de Financiamento', 'PK': 'Parcela de Financiamento', 'P003': 'Diferença de Financiamento', 'X001': 'Parcela de FGTS', 'XK': 'Parcela de FGTS', 'K001': 'Entrada do Contrato Kit Acabamento', 'K': 'Kit Fixo', 'KC': 'Kit Fixo (carência sinal)', 'H': 'Recurso Próprio', 'NH': 'Recurso Próprio', 'M500': 'Parcela de quitação.', 'TI': 'Será criada para conceder desconto referente a Troca de Índice, concedida por Matriz de Alçada, Autorizações Especiais, Acordo Jurídico', 'IH': 'Será criada para informar a variação do índice de reajuste devido a inserção da Data do Habite-se no contrato', 'FP': 'Parcela de Desconto na Venda (Promoção do Comercial)', 'E100': 'Parcela utilizada para baixar as diferenças de financiamento (P003 ou B003) para liquidação do contrato (não ficará saldo devedor) sem a necessidade de aditivos.', 'E300': 'Parcela utilizada para baixar as diferenças de financiamento P003 ou B003 pago a vista (Cliente Inadimplente)', 'DF': 'Parcela de Diferença de Financiamento (cliente adimplente)', '@': 'Parcela de Diferença de Financiamento renegociada, ou seja, parcelas DF que são renegociadas passam a ser @', 'DF99': 'Parcela de Diferença de Financiamento (cliente inadimplente) sistema SAP 6.0', 'E500': 'Parcela de Diferença de Financiamento (cliente inadimplente) sistemas SAP 4.5 e Netterm', 'E120': 'Entrada do parcelamento da Diferença de Financiamento (cliente inadimplente)', 'B005': 'Diferença de Financiamento', 'P005': 'Diferença de Financiamento', 'IN': 'Inadimplência CEF (SAP 6.0)', 'E900': 'Inadimplência CEF (SAP 4.5)', 'I': 'Taxa de Despachante', 'R': 'Registro / Escritura', 'RF': 'FUNREJUS E/OU LAUDÊMIO', 'RI': 'ITBI', 'RT': 'Registro de Cartório', 'RS': 'Taxa de Financiamento SBPE', 'RR': 'Taxa de Alocação de Recurso', 'RC': 'Parcela de ITBI para contratos Flex.', 'RE': 'Parcela de Registro de Cartório para contratos Flex.', 'AU': 'Reembolso de taxa de atribuição de unidade A taxa de atribuição de unidade é o valor cobrado pelo cartório para a averbação do Habite-se do residencial, onde cada um dos imóveis é registrado individualmente.', 'U': 'IPTU - O IPTU é cobrado pelas prefeituras tendo como referência o lote, os departamentos responsáveis pelas quitações são o desenvolvimento imobiliário (até liberação do RI) e engenharia (após a liberação do RI e até o desdobro).', 'UC': 'IPTU (Pago pos recebimento de chaves para liberação de CND)', 'UD': 'IPTU (Exclusivo CND)', 'Y': 'Taxa de Administração', 'T': 'Cessão de Direitos Total', 'D': 'Desconto Promocional', 'C': 'Condomínio', 'CD': 'Condomínio (Exclusivo CND)', 'G': 'Casa Garantia', 'O': 'Notificação', 'S': 'Seguro Garantia (Obra)', 'TE': 'É a recuperação de valor referente a taxa enfiteuse incidente em imóveis localizados em terrrenos da marinha', 'MP': 'A parcela MP (Recuperação FP) será criada com o objetivo de cobrar a parcela FP caso o cliente não cumpra com as condições do regulamento da promoção Cliente Bom Pagador.', 'DK': 'Desconto Promocional de Kit', 'E400': 'Será criada nos casos em que o cliente era enquadrado no Minha casa minha vida e perdeu o subsidio do governo devido empreendimentos que tiveram problemas e ficaram embargados, Nesses casos, a MRV irá arcar com o subsídio.', 'E200': 'Parcela utilizada para baixar as diferenças de financiamento P003 ou B003 paga a vista (Cliente Adimplente)', 'Z': 'Contribuição', 'Q': 'Kit Corrigido', 'F': 'Atualização do Financiamento Bancário', 'V': 'Chaves'}
            
            descriptionOfType = parcelTypeDescriptionMapping.get(row['Tipo da Parcela'], row['Tipo da Parcela'])
           
            parcelType = contractTypeMapping.get(row['Tipo da Parcela'], row['Tipo da Parcela'])
            
            # Parse dias atraso
            # This is not a value we will get from the csv, but we will calculate it
            #this is relative to the current date and the field 'Data de Vencimento da Parcela'
            daysOverdue = 0
            if dueDate is not None:
                daysOverdue = (datetime.now().date() - dueDate).days
            

            
            lastUpdatedDate = datetime.now().date()


            parcelDescription = row['Descrição da Parcela'] if pd.notna(row['Descrição da Parcela']) else ''
            parcelIdentifier = row['Identificador da Parcela'] if pd.notna(row['Identificador da Parcela']) else ''

            #Data relative to the payment of the parcela will be inserted in the table (was not previously), value of last boleto, last boleto creation(date), status of the last boleto
            #We want the fields 'VALOR DO ÚLTIMO BOLETO', 'ÚLTIMO BOLETO CRIADO', 'STATUS DO ÚLTIMO BOLETO' to be inserted in the table
            lastBoletoValue = convertCurrencyToFloat(row['VALOR DO ÚLTIMO BOLETO'])
            lastBoletoDate = (
                datetime.strptime(row['ÚLTIMO BOLETO CRIADO'], '%Y-%m-%d')
                        .date()
                if pd.notna(row['ÚLTIMO BOLETO CRIADO']) 
                else None
            )
            lastBoletoStatus = row['STATUS DO ÚLTIMO BOLETO'] if pd.notna(row['STATUS DO ÚLTIMO BOLETO']) else ''
            
            #bool for when the diasAtraso is > 0 beacause its overdue, so the client must be charged(thiefs)
            active = daysOverdue > 0 and lastBoletoStatus.lower() != 'pago'

            sqlQuery = """ INSERT INTO Parcela (
                numeroContrato, codigoParcela, tipoEspecifico, tipoParcela, descricaoParcela,
                valorOriginal, valorAtualizado, valorPendente, dataVencimento,
                dataAtualizacao, statusParcela, diasAtraso, ativa, dataCriacaoUltimoBoleto, valorUltimoBoleto, statusUltimoBoleto)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ;"""
            
            #execute
            cursor.execute(sqlQuery, (
                contratoCode,
                parcelIdentifier,
                parcelType,
                descriptionOfType,
                parcelDescription,
                initialParcelValue,
                currentParcelValue,
                pendingValue,
                dueDate,
                lastUpdatedDate,
                parcelStatus,
                daysOverdue,
                active,
                lastBoletoDate,
                lastBoletoValue,
                lastBoletoStatus
            ))
    #commit after all data of this chunk is inserted
    conn.commit()
    cursor.close()

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