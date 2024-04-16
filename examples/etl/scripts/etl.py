import mysql.connector as connection
import pandas as pd
import time
from sqlalchemy import create_engine
import psycopg2


try:
    # =========== Read data from MySQL =================
    mydb = connection.connect(host="localhost", database = 'operational', user="root", passwd="mysql", use_pure=True, port=3305)

    query = "Select * from youtube;"
    result_dataFrame = pd.read_sql(query, mydb)
    print(result_dataFrame)

    # =========== Write data to Postgresql =================
    #INPUT YOUR OWN CONNECTION STRING HERE
    conn_string = 'postgresql://postgres:postgres@localhost/data_warehouse'

    #perform to_sql test and print result
    db = create_engine(conn_string)
    conn = db.connect()

    start_time = time.time()
    result_dataFrame.to_sql('youtube_etl', con=conn, if_exists='replace', index=False)
    print("to_sql duration: {} seconds".format(time.time() - start_time))

    mydb.close() #close the connection
except Exception as e:
    mydb.close()
    print(str(e))

