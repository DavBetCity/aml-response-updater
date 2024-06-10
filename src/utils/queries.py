###### SELECTED COLUMNS
############################################################################################
############################################################################################
def get_selected_columns():
    return """
        betrid,
        current_list,
        curr_list_date
    """


############################################################################################
###### QUERY ALL REGISTRY PLAYERS
############################################################################################
def get_all_colnames_query():
    return f"""
                SELECT 
                    {get_selected_columns()}
                FROM registry_player 
                WHERE 1=0
            """


############################################################################################
###### QUERY ALL REGISTRY PLAYERS
############################################################################################
def query_all_registry_players():
    return f"""
                SELECT 
                    {get_selected_columns()}       
                FROM registry_player
            """


############################################################################################
###### QUERY ALL REGISTRY PLAYERS
############################################################################################
def query_all_registry_players():
    return f"""
                SELECT 
                    {get_selected_columns()}       
                FROM registry_player
            """


############################################################################################
###### QUERY PLAYERS FOR MI REPORTING
############################################################################################
def query_mi_players():
    return f"""
                SELECT 
                    betrid,
                    current_list,
                    curr_list_date,
                    total_hits,
                    first_hit,
                    last_hit,
                    picked_up_by_id,
                    trigger_text,
                    risk_level,
                    account_state
                FROM registry_player
            """

############################################################################################
###### QUERY USERS FOR MI REPORTING
############################################################################################
def query_mi_users():
    return f"""
                SELECT 
                   id,
                   username,
                   email,
                   first_name,
                   last_name,
                   is_superuser,
                   is_active
                FROM core_user
            """

############################################################################################
###### QUERY USER LOG FOR MI REPORTING
############################################################################################
def query_mi_userlog():
    return f"""
                SELECT 
                   *
                FROM core_userlog
            """

############################################################################################
###### QUERY PLAYER LOG FOR MI REPORTING
############################################################################################
def query_mi_playerlog():
    return f"""
                SELECT 
                   *
                FROM registry_playerlog
            """
