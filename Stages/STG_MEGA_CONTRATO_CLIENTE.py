from datetime import datetime
from .SQL import CONTRATO_CLIENTE 

TABLE_NAME = "STGBILD.STG_MEGA_CONTRATO_CLIENTE"

DATA_PROCESSAMENTO = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_select(cons):
    return CONTRATO_CLIENTE
