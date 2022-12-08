from datetime import datetime
from .SQL import AGENTES 

TABLE_NAME = "STGBILD.STG_MEGA_AGENTES"

DATA_PROCESSAMENTO = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_select(cons):
    return AGENTES
