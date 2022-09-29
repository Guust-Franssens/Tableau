import pandas as pd
import os
import glob
import time
import datetime
import re
import json
import shutil
import psycopg2
import warnings
from .flattened_dataframe import FlattenedDataFrame
from .ts_config import generate_config
from tableau_api_lib import TableauServerConnection
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

current_date = time.strftime("%Y-%m-%d")
TABLEAU_DATABASE_CONNECTION_DETAILS = "SERVERNAME:8060/workgroup?"
DEFAULT_REST_ENV = "XXXXXXXX"

LOOKUP_RESPONSES = {
    "sign_in": {
        200: "Signed in succesfully",
        400: "Bad Request: The content of the request body is missing or incomplete, or contains malformed XML.",
        401: "Login error: Invalid credentials. Make sure the provided Personal Access Token (PAT) is correct.",
        405: "Invalid request method: method was not POST"
    },
    "download_workbook": {
        400: "There was a problem downloading or querying this file",
        401: "The authentication token for the request is missing, invalid, or expired",
        403: "A user attempted to download a .xlsx file without Read and/or ExportData permissions for the workbook or view, and is not an administrator.",
        404: "The site or view specified in the request could not be found.",
        405: "Request type was not GET."
    },
    "download_data_source": {
        403: "A non-administrator user attempted to download a data source, but the caller doesn't have Read permission.",
        404: "The data source ID in the URI doesn't correspond to an existing data source.",
        405: "Request type was not GET."
    }
}

class LoginError(Exception):
    pass
    
class ValueNotFoundError(Exception):
    pass

def open_json(path):
    with open(path) as json_file:
        data = json.load(json_file)  
    return data
    

def unpack_response(to_unpack_dict):
    if len(to_unpack_dict) == 1:
        for k, v in to_unpack_dict.items():
            if isinstance(v, dict):
                return unpack_response(v)
            elif isinstance(v, list):
                return v
            else:
                breakpoint()
                raise TypeError
    else:
        message = (
            f"response contained multiple messages:\n" 
            f"{to_unpack_dict.get('errors', '')}\n\b" 
            f"Keys in the message are the following:\n" 
            f"\t{to_unpack_dict.keys()}" 
        )
        raise Exception(message)


def create_folder(path:str):
    print(f"\n{path} does not exist, creating this folder...", end=" ")
    os.makedirs(path)
    print("done!")


def move_to_historiek(folder:str):
    if not os.path.exists(folder):
        create_folder(folder)
        
    historiek_folder = os.path.join(folder, "historiek")

    if not os.path.exists(historiek_folder):
        create_folder(historiek_folder)
    
    files = glob.glob(f"{folder}\\*.*")
    p_file_name = r"(?<=\\)\w+\.(csv|xlsx)"
    if files:
        print("\nStarted moving files to historiek")
    for file in files:
        creation_time = datetime.datetime.fromtimestamp(os.path.getctime(file)).date().__str__()
        try: 
            file_name, extension = re.search(p_file_name, file)[0].split('.')
            new_file_name = f"{file_name}-{creation_time}.{extension}"

        except TypeError:
            print(f"file name could not be found for {file}")
            print(f"skipping moving this file")
            continue
        
        destination = os.path.join(historiek_folder, new_file_name)
        shutil.move(file, destination)
    
    print("Done moving files to historiek\n")


def move_file_to(file:str, to:str):
    if not os.path.exists(file):
        raise Exception(f"no file was found at {file}")
    
    if not os.path.exists(to):
        create_folder(to)
    
    destination = os.path.join(to, os.path.basename(file))
    shutil.move(file, destination) 
    

def convert_query(folder, connection, query_name, query) -> None:
    print("\n-----------------------------------------------------------") 
    print(f"Executing query: {query_name} \n")
    
    response = connection.metadata_graphql_query(query=query).json()

    try: 
        unpacked_response = unpack_response(response)

    except Exception as err:
        print(f"The query '{query_name}' did not return a valid response and resulted in the following error:\n{err}")
        print("-----------------------------------------------------------\n")
        return
         
    df = FlattenedDataFrame(unpacked_response)
    print(f"Saving file at {folder}: '{query_name}_{connection._env}.xlsx'")
    df.to_excel(f"{folder}\\{query_name}_{connection._env}.xlsx", index=False, sheet_name=query_name)
    print("Finished running query")
    print("-----------------------------------------------------------\n") 

def lookup_error(method:str, status:int):
    method_statuses = LOOKUP_RESPONSES.get(method)
    if method_statuses is None:
        raise NotImplementedError(f"The method {method} has not yet been implemented. \nHere is a list of all implemented methods: {LOOKUP_RESPONSES.keys()}")
    
    method_status = method_statuses.get(status)
    if method_status is None:
        raise ValueNotFoundError(f"The status code {status} is not found for method {method}. \nHere is a list of the current statuses: {method_status.keys()}")
    
    return method_status
    
def prompt_personal_access_token(environment:str="tableau_prod") -> dict: 
    print(f"\nTo log into Tableau Server {environment} a Personal Access Token is needed")
    pat_name = input("\tName of the Personal Access Token: ")
    pat_secret = input(f"\tThe secret code generated for '{pat_name}': ")
    print("")
    return {"pat_name": pat_name, "pat_secret": pat_secret}

def prompt_tableau_database_credentials() -> dict:
    print("\nTo execute this script we need to log into the Tableau PRD database. Please provide the credentials.")
    username = input("\tUsername: ")
    password = input("\tPassword: ")
    print("")
    return {"username": username, "password": password}
    
def setup_database_connection():
    response = prompt_tableau_database_credentials()
    database_connection_string = f"postgresql+psycopg2://{response.get('username')}:{response.get('password')}@{TABLEAU_DATABASE_CONNECTION_DETAILS}"
    alchemyEngine = create_engine(database_connection_string, pool_recycle=3600)
    
    try: 
        print("Attempting to sign into Tableau database...")
        dbConnection = alchemyEngine.connect()
        print("Signed in succesully!\n")
        return dbConnection
    except OperationalError:
        error = (
            "\n\n----------------------------------------------------------------------------------------------------------------------------------------------------",
            "\t\t\t\t\t\t\t\t\t!ERROR!\n",
            "Something went wrong when attempting to query the Tableau Database. Check if the inputted credentials are correct, and that the server is running",
            "-----------------------------------------------------------------------------------------------------------------------------------------------------\n"
        )
        print(*error, sep="\n")
        raise LoginError("Error whilst attempting to login to the database") from None
        

def setup_REST_connection(config_file:str, environment:str=DEFAULT_REST_ENV):
    try:
        ts_config = open_json(config_file)
    except FileNotFoundError:
        print(f"REST API config file not found {config_file}. Generating the config file...", end=" ")
        ts_config = generate_config()
        print("done!")
    
    if ts_config.get(environment).get("personal_access_token_name") == "<YOUR_USERNAME>":
        credentials = prompt_personal_access_token(environment)
        ts_config[environment]["personal_access_token_name"] = credentials.get("pat_name")
        ts_config[environment]["personal_access_token_secret"] = credentials.get("pat_secret")
    
    with warnings.catch_warnings(): # we get a warning that no ssl is inplace. We ignore this warning since this is due to the company proxy
        warnings.simplefilter("ignore")
        conn = TableauServerConnection(ts_config, ssl_verify=False, env=environment)
    
        try:
            print("Attempting to sign into Tableau REST API...")
            response_code = conn.sign_in().status_code
            if response_code != 200:  # success code voor connectie
                
                raise ValueError
            print("Signed in succesully!\n")    
            return conn
        except ValueError:
            error = lookup_error("sign_in", response_code)
            msg=(
                f"\n-------------------------------------------------------------------------------------------------",
                f"Signing into the Tableau server FAILED!",
                f"{error}",
                f"-------------------------------------------------------------------------------------------------\n"
            )
            print(*msg, sep="\n")
            raise LoginError("Error whilst attempting to login to the REST API") from None
            
            