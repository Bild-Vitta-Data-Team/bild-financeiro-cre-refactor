from datetime import datetime
from .SQL import CLASSIFICACAO_CONTRATO 

TABLE_NAME = "STGBILD.STG_MEGA_CLASSIFICACAO_CONTRATO"

DATA_PROCESSAMENTO = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_select(cons):
    return CLASSIFICACAO_CONTRATO
