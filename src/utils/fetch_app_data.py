import pandas as pd
from db.bq import BQ
from db.pgsql import connect_postgres
from utils.queries import (
    query_mi_players,
    query_mi_users,
    query_mi_userlog,
    query_mi_playerlog,
)


############################################################################################
###### GET DATA COLUMNS
############################################################################################
def get_data_columns(cursor, query):
    cursor.execute(f"{query()} WHERE 1=0")

    return [desc[0] for desc in cursor.description]


############################################################################################
###### GET DATA
############################################################################################
def get_data(cursor, query):
    columns = get_data_columns(cursor, query)
    cursor.execute(query())

    return pd.DataFrame(cursor.fetchall(), columns=columns)


############################################################################################
###### FETCH UPLOAD PSQL DATA
############################################################################################
def fetch_upload_psql_data():
    # Connect to Postgres
    conn, cursor = connect_postgres()

    players = get_data(cursor, query_mi_players)
    users = get_data(cursor, query_mi_users)
    userlog = get_data(cursor, query_mi_userlog)
    playerlog = get_data(cursor, query_mi_playerlog)

    conn.close()

    return players, users, userlog, playerlog


############################################################################################
###### FETCH UPLOAD DATA AML APP DATA
############################################################################################
def fetch_upload_data_aml_app_data(local):
    bq = BQ(local=local)
    base_name = f"betcity-319812.aml_app.mi_"

    players, users, userlog, playerlog = fetch_upload_psql_data()

    bq.create_replace_table(players, f"{base_name}players")
    bq.create_replace_table(users, f"{base_name}users")
    bq.create_replace_table(userlog, f"{base_name}userlog")
    bq.create_replace_table(playerlog, f"{base_name}playerlog")

    print("Data uploaded to BigQuery")
