try:
    import os
    import warnings
    import time
    import argparse
    from tableau_api_lib import TableauServerConnection
    from utils import queries, convert_query, move_to_historiek, setup_REST_connection
except ModuleNotFoundError as e:
    print("\n\n--------------------------------------------------------------------------------------------------------------\n")
    print("The python environment you are running this script in does not contain all the dependencies.")
    print("\n--------------------------------------------------------------------------------------------------------------\n\n")
    raise Exception(str(e)) from None

TS_CONFIG_NAME = "ts_config"
OUTPUT_FOLDER = "output\\metadata_api"
DEFAULT_ENV = ""

def main(environment):   
    with warnings.catch_warnings(): # we get a warning that no ssl is inplace if the company has a proxy
        warnings.simplefilter("ignore")
        
        start_time = time.time()
        
        # establishing connection with REST API
        conn = setup_REST_connection(f"{TS_CONFIG_NAME}.json", environment=environment)
        
        # move existing files to historiek
        move_to_historiek(folder=OUTPUT_FOLDER)

        # run queries and sign out
        for query_name, query in queries.items():
            convert_query(OUTPUT_FOLDER, conn, query_name, query)
        conn.sign_out()

        end_time = time.time()
        print("Signed out succesfully!")
        print(f"It took {int(round(end_time - start_time, 0))} seconds to run the program")

            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='A script that allows for metadata extraction by using graphql queries')
    parser.add_argument(
        "--environment",
        help="Environment to run the extraction in. options: tableau_prod_{site name} & tableau_sim_{site name}.",
        default=DEFAULT_ENV
    )
    args = parser.parse_args()
    
    # setting to root directory after imports
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    
    main(args.environment)