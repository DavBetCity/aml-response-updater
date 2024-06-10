import os
from dotenv import load_dotenv
from utils.decorators import time_counter
from utils.response_updater import response_updater
from utils.fetch_app_data import fetch_upload_data_aml_app_data

load_dotenv()


############################################################################################
###### MAIN
############################################################################################
@time_counter
def main(message=None):
    local = os.environ.get("ENVIRONMENT") == "development" or False

    # Response Updater
    response_updater(local)

    # Fetch App Data
    fetch_upload_data_aml_app_data(local)

    return "Success"


if __name__ == "__main__":
    main()
