
from datetime import datetime
from warnings import filterwarnings
from pyspark.sql import SparkSession
from setup_stages import PIPE

from Utils import (
    SYS_BD, 
    SYS_HOST, 
    SYS_PORT, 
    SYS_USER, 
    SYS_PSWD,
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

URL_MYSQL = f"jdbc:mysql://{SYS_HOST}:3306/{SYS_BD}?useSSL=true&user={SYS_USER}&password={SYS_PSWD}"
URL_SQL_SERVER = f"jdbc:sqlserver://{SSMS_HOST}:1433;databaseName={SSMS_DB};"
URL_ORACLE_MEGA = f"jdbc:oracle:thin:@{ORACLE_MEGA_HOST}:{ORACLE_MEGA_PORT}/{ORACLE_MEGA_SERVICE}"

cons = dict(
    SYS_BD=SYS_BD, 
    SYS_HOST=SYS_HOST, 
    SYS_PORT=SYS_PORT, 
    SYS_USER=SYS_USER, 
    SYS_PSWD=SYS_PSWD,
    SSMS_DB=SSMS_DB,
    SSMS_HOST=SSMS_HOST,
    SSMS_USER=SSMS_USER,
    SSMS_PSWD=SSMS_PSWD,
    URL_MYSQL=URL_MYSQL,
    URL_SQL_SERVER=URL_SQL_SERVER,
    URL_ORACLE_MEGA=URL_ORACLE_MEGA,
    ORACLE_MEGA_USER=ORACLE_MEGA_USER,
    ORACLE_MEGA_PSWD=ORACLE_MEGA_PSWD
)


spark = SparkSession.builder.config("spark.driver.memory", "12g").appName("Stage").getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enables", "true")


print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [MAIN]  | EXTRACTION_STARTED", file=open("./geral_logs.txt", "a"))


for pipe in PIPE:
    base, project_name, workflow = pipe["JOB"], pipe["NAME"], pipe["PROCESS"]
    base_name = base.__name__

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [START] | {base_name} -> {project_name} | SCRIPT", file=open("./geral_logs.txt", "a"))

    # Setup Base Class
    _base = base(project_name)

    # Get Custom Select
    select = workflow.get_select(cons)

    # Get Spark Dataframe
    check, dataframe = _base.data_load(spark, select, cons)

    if not check:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [ERROR] | {base_name} -> {project_name} | DATA_LOAD\n", file=open("./geral_logs.txt", "a"))
        print(f"{dataframe}\n", file=open("./geral_logs.txt", "a"))
        break

    else:
        dataframe = _base.data_preprocessing(dataframe)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [INFO]  | {base_name} -> {project_name} | DATA_CLEANED\n", file=open("./geral_logs.txt", "a"))
        
        check, status = _base.data_storange(dataframe, cons)

        if not check:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [ERROR] | {base_name} -> {project_name} | DATA_STORANGE\n", file=open("./geral_logs.txt", "a"))
            print(f"{status}\n", file=open("./geral_logs.txt", "a"))
            break
            
	else:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [INFO]  | {base_name} -> {project_name} | DATA_STORED\n", file=open("./geral_logs.txt", "a"))


print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [MAIN]  | EXTRACTION_ENDED\n", file=open("./geral_logs.txt", "a"))
