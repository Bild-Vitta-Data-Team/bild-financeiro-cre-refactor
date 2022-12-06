from datetime import datetime
from pandas import read_sql_query
from sqlalchemy import create_engine
from pyspark.sql.functions import col, lit
from . import PARCELA_RECEITA

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

def data_load(spark, select, cons):
    try:
        
        df = (
            spark.read
            .format('jdbc')
            .option('url', cons['URL_ORACLE_MEGA'])
            .option('dbtable', select)
            .option('user', cons['ORACLE_MEGA_USER'])
            .option('password', cons['ORACLE_MEGA_PSWD'])
            .option('driver', 'oracle.jdbc.driver.OracleDriver')
            #.option('spark.jars', 'Jars/ORACLE_JARS')
            .load()
        )

    except:
        return False

    return df

def data_preprocessing(df):
    df = df.select([col(k).cast('String') for k in df.columns])
    df = df.withColumn('DATA_PROCESSAMENTO', lit(DATA_PROCESSAMENTO))

    return df

def data_storange(df, cons):
    try:
                
        (
            df.write
            .format('jdbc')
            .mode('overwrite')
            .option('url', cons['URL_SQL_SERVER'])
            .option('dbtable', TABLE_NAME)
            .option('user', cons['SSMS_USER'])
            .option('password', cons['SSMS_PSWD'])
            # Checkout and Download Drivers
            #.option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')
            #.option('spark.jars', 'Jars/spark-mssql-connector_2.12_3.0-1.0.0-alpha.jar')
            .save()
        )
        
    except:
        return False

    return True