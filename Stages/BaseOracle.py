from datetime import datetime
from pyspark.sql.functions import col, lit


class BaseOracle():
    def __init__(self, table_name):
        self._table_name = table_name
        self.data_processamento = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


    def data_load(self, spark, select, cons):

        try:
            
            df = (
                spark.read
                .format('jdbc')
                .option('url', cons['URL_ORACLE_MEGA'])
                .option('dbtable', select)
                .option('user', cons['ORACLE_MEGA_USER'])
                .option('password', cons['ORACLE_MEGA_PSWD'])
                .option('driver', 'oracle.jdbc.driver.OracleDriver')
                .load()
            )

        except Exception as exp:
            return False, exp

        return True, df

    
    def data_preprocessing(self, df):
        df = df.select([col(k).cast('String') for k in df.columns])
        df = df.withColumn('DATA_PROCESSAMENTO', lit(self.data_processamento))

        return df


    def data_storange(self, df, cons):

        try:
                    
            (
                df.write
                .format('jdbc')
                .mode('overwrite')
                .option('url', cons['URL_SQL_SERVER'])
                .option('dbtable', self._table_name)
                .option('user', cons['SSMS_USER'])
                .option('password', cons['SSMS_PSWD'])
                .save()
            )

            del df
            
        except Exception as exp:
            return False, exp

        return True, None