import pandas as pd
from db.bq import BQ
from db.pgsql import connect_postgres
from utils.queries import query_all_registry_players, get_all_colnames_query


############################################################################################
###### GET COLNAMES
############################################################################################
def get_colnames(cursor):
    cursor.execute(get_all_colnames_query())

    return ", ".join([desc[0] for desc in cursor.description])


############################################################################################
###### GET ALL PLAYERS
############################################################################################
def get_all_players(cursor):
    colnames = get_colnames(cursor)
    cursor.execute(query_all_registry_players())

    return pd.DataFrame(
        cursor.fetchall(),
        columns=colnames.split(", "),
    )


############################################################################################
###### DIVIDE PLAYERS BY LIST
############################################################################################
def annotate_players_data(players):
    # Database filter values
    APPROVED = "APPROVED"
    NEWLYADDED = "NEWLYADDED"

    # Status values
    STARTED = "started"
    FINISHED = "finished"
    NOTSTARTED = "notstarted"
    AMLAPPEMAIL = "aml-app@sportsentmedia.com"

    # populate email_agent
    players["email_agent"] = AMLAPPEMAIL

    def map_list_to_status(curr_list):
        if curr_list == NEWLYADDED:
            return NOTSTARTED
        elif curr_list == APPROVED:
            return FINISHED
        else:
            return STARTED

    # Map list to status
    players["status"] = players["current_list"].apply(map_list_to_status)


############################################################################################
###### GET ALL PLAYERS
############################################################################################
def get_player_data():
    conn, cursor = connect_postgres()
    df = get_all_players(cursor)
    annotate_players_data(df)

    conn.close()
    return df


############################################################################################
###### MAIN
############################################################################################
def main(message=None):
    bq = BQ(local=False)
    players = get_player_data()
    table_name = "registry_player_logs"
    bq_table = f"betcity-319812.aml_app.{table_name}"

    # Make response table structure
    df = players.loc[:, ["curr_list_date", "email_agent", "betrid", "status"]]
    df.rename(
        columns={"curr_list_date": "timestamp_placed", "betrid": "username"},
        inplace=True,
    )

    # Timestamp to UTC format as response sheet
    df["timestamp_placed"] = pd.to_datetime(df["timestamp_placed"])
    df["timestamp_placed"] = (
        df["timestamp_placed"].dt.strftime("%Y-%m-%d %H:%M:%S.%f") + " UTC"
    )

    # Create/Replace table
    bq.create_replace_table(df, bq_table)

    return "Success"


if __name__ == "__main__":
    main()
