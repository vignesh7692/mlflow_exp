# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import mlflow
from pyspark import SparkContext
from pyspark.sql import SparkSession

def log_delta():
    spark = SparkSession.builder.getOrCreate()
    data = spark.sql("select * from freddy_fm_d2c.d2c_accounts where mcr_account_id = 1511679284073906176 ").toPandas()
    accid = data['mcr_account_id']
    print(accid)
    mlflow.log_param("acc_id", accid)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    log_delta()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
