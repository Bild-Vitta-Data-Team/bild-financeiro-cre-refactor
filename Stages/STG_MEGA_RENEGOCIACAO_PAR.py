from datetime import datetime
from .SQL import RENEGOCIACAO_PAR 

TABLE_NAME = "STGBILD.STG_MEGA_RENEGOCIACAO_PAR"

DATA_PROCESSAMENTO = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_select(cons):
    end_date = datetime.now().strftime('%d/%m/%y')
    start_date = end_date.replace(end_date[:2], '01')

    select_table = RENEGOCIACAO_PAR.format(start_date, end_date)

    return select_table
