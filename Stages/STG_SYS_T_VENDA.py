from datetime import datetime
from pyspark.sql.functions import col, lit
from .SQL import VENDAS 

TABLE_NAME = "STGBILD.STG_SYS_T_VENDA"

DATA_PROCESSAMENTO = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_select(cons):
    return VENDAS
