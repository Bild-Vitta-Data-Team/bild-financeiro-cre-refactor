
from datetime import datetime
from warnings import filterwarnings
from pyspark.sql import SparkSession
from setup_stages import PIPE

from Utils import (
    SSMS_HOST,
    SSMS_DB,
    SSMS_USER,
    SSMS_PSWD,
    ORACLE_MEGA_HOST,
    ORACLE_MEGA_PORT,
    ORACLE_MEGA_USER,
    ORACLE_MEGA_PSWD,
    ORACLE_MEGA_SERVICE,
)

filterwarnings('ignore')

URL_ORACLE_MEGA = f"jdbc:oracle:thin:@{ORACLE_MEGA_HOST}:{ORACLE_MEGA_PORT}/{ORACLE_MEGA_SERVICE}"
URL_SQL_SERVER = f"jdbc:sqlserver://{SSMS_HOST}:1433;databaseName={SSMS_DB};"

cons = dict(
    SSMS_DB=SSMS_DB,
    SSMS_HOST=SSMS_HOST,
    SSMS_USER=SSMS_USER,
    SSMS_PSWD=SSMS_PSWD,
    URL_SQL_SERVER=URL_SQL_SERVER,
    URL_ORACLE_MEGA=URL_ORACLE_MEGA,
    ORACLE_MEGA_USER=ORACLE_MEGA_USER,
    ORACLE_MEGA_PSWD=ORACLE_MEGA_PSWD
)

spark = SparkSession.builder.config("spark.driver.memory", "12g").appName("Stage").getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enables", "true")


print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [MAIN] | EXTRACTION_STARTED", file=open("./stages_logs.txt", "a"))

for name, workflow_stage in PIPE.items():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [START] | {name} | SCRIPT", file=open("./stages_logs.txt", "a"))
    
    select = workflow_stage.get_select(cons)
    
    dataframe = workflow_stage.data_load(spark, select, cons)
    
    if dataframe:
        dataframe = workflow_stage.data_preprocessing(dataframe)
    
        if not workflow_stage.data_storange(dataframe, cons):
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [ERROR] | {name} | DATA_STORANGE\n", file=open("./stages_logs.txt", "a"))
    
    else: 
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [ERROR] | {name} | DATA_LOAD\n", file=open("./stages_logs.txt", "a"))
    
    
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [ENDED] | {name} | SCRIPT\n", file=open("./stages_logs.txt", "a"))

print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [MAIN] | EXTRACTION_ENDED\n", file=open("./stages_logs.txt", "a"))