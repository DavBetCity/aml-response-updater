############################################################################################
###### SELECTED COLUMNS
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
