from datetime import datetime
from pyspark.sql.functions import col, lit
from . import AGENTES 

TABLE_NAME = "STGBILD.STG_MEGA_AGENTES"

DATA_PROCESSAMENTO = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_select(cons):
    return AGENTES

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