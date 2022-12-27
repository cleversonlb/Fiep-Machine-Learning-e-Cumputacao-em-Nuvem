from sqlalchemy import create_engine
import pandas as pd
import psycopg2

df = pd.read_csv('https://raw.githubusercontent.com/maxreis86/FIEP-Machine-Learning-e-Computacao-em-Nuvem/main/aula_01_titanic_h2o_automl/titanic/dataprep_df.csv')

engine = create_engine('postgresql://user:password@fiep-postgres.postgres.database.azure.com:5432/postgres')
df.to_sql('titanic', engine, if_exists='replace')

print(pd.read_sql_query('select * from titanic', engine))