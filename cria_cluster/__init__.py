import datetime
import logging
import h2o
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

import azure.functions as func
import os

#install java
os.system('apt install -y openjdk-8-jdk')


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    # if mytimer.past_due:
    #     logging.info('The timer is past due!')

    # logging.info('Python timer trigger function ran at %s', utc_timestamp)

    try:
        engine = create_engine('postgresql://cleversonadao@postgres-portal:Postgres2022@postgres-portal.postgres.database.azure.com:5432/postgres')

        dataprep_df = pd.read_sql_query('select * from titanic', engine)

        #Confusion Matrix for the Champion
        df_cluster_tmp = h2o.mojo_predict_pandas(dataprep_df, mojo_zip_path='KMeans_model_python_1670002330623_1.zip', verbose=False)

        df_cluster = pd.concat([df_cluster_tmp.reset_index(drop=True), dataprep_df.reset_index(drop=True)], axis=1)
        df_cluster['cluster'] = df_cluster['cluster'].astype(int)

        df_cluster.to_sql('titanic_cluster', engine, if_exists='replace')

        print(pd.read_sql_query('select * from titanic_cluster', engine))
        print ("Rodou com sucesso")

    except Exception as e:
        logging.info(e)
        print(e)