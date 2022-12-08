from datetime import datetime
from pandas import read_sql_query
from sqlalchemy import create_engine
from .SQL import PARCELA_RECEITA

TABLE_NAME = "STGBILD.STG_MEGA_PARCELA_RECEITA"

DATA_PROCESSAMENTO = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_aux_data(cons):
    string_con = f"mssql+pyodbc:///?odbc_connect=DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={cons['SSMS_HOST']};DATABASE={cons['SSMS_DB']};UID={cons['SSMS_USER']};PWD={cons['SSMS_PSWD']}"

    db = create_engine(string_con)

    con = db.connect()

    codes = read_sql_query("select distinct CODIGO_EMPREENDIMENTO from DMGeral.D_EMPREENDIMENTO where SK_D_GRUPO_EMPRESA = 5", con=con)['CODIGO_EMPREENDIMENTO'].tolist()
    codes = ", ".join([str(k) for k in codes])

    con.close()

    return codes

def get_select(cons):
    end_date = datetime.now().strftime('%d/%m/%y')
    start_date = end_date.replace(end_date[:2], '01')

    codes_filial = get_aux_data(cons)

    select_table = PARCELA_RECEITA.format(start_date, codes_filial)

    return select_table
