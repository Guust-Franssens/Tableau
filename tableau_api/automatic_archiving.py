try:
    import os, sys
    import win32com.client as win32
    import re
    import pandas as pd
    import warnings
    import time
    from zipfile import ZipFile, ZIP_DEFLATED
    from argparse import ArgumentParser
    from utils import lookup_error, setup_database_connection, setup_REST_connection, query_orphan_datasources, query_unused_workbooks, move_file_to
except ModuleNotFoundError as e:
    print("\n\n--------------------------------------------------------------------------------------------------------------\n")
    print("The python environment you are running this script in does not contain all the dependencies.")
    print("\n--------------------------------------------------------------------------------------------------------------\n\n")
    raise Exception(str(e)) from None


class IDNotFoundError(Exception):
    pass

try:
    outlook = win32.Dispatch('outlook.application')
except:
    print("Please open outlook")
    
def setup_mail(item_name:str, item_type:str, owner:str, email:str, tableau_location:str, attachment_location:str) -> None:
    mail = outlook.CreateItem(0)
    mail.Subject = f"Tableau: {item_type} - {item_name}"
    mail.SentOnBehalfOfName = "EMAILADDRESSTOSENDFROM"
    mail.to = email
    mail.HTMLBody = (
        fr"""
        HERE STRUCTURE YOUR EMAIL IN HTML FORMAT
        """
    )
    mail.Attachments.Add(os.path.join(os.getcwd(), attachment_location))
    mail.Save()
    

def cleanup_workbooks(dbConnection, REST_conn, delete=False) -> pd.DataFrame:
    if delete:
        print("Are you sure you want to delete the workbooks? type 'continue' to continue or 'quit' to quit")
        breakpoint()
    df_w = pd.read_sql(query_unused_workbooks(), dbConnection)
    df_w["id"] = df_w["id"].astype(str)
    df_w = df_w[df_w["project site name"] == REST_conn.site_name]
    
    for index, row in df_w.iterrows():
        attachment_location = cleanup_item(row["id"], "workbook", REST_conn.download_workbook, REST_conn.delete_workbook if delete else None)
        setup_mail(
            item_name=row["workbook name"],
            item_type="workbook",
            owner=row["owner"],
            email=row["email"],
            tableau_location=row["project path"],
            attachment_location=attachment_location
        )
        move_file_to(attachment_location, BACKUP_LOCATION)
    
    return df_w
    
def cleanup_datasouces(dbConnection, REST_conn, delete=False) -> pd.DataFrame:
    if delete:
        print("Are you sure you want to delete the data sources? type 'continue' to continue or 'quit' to quit")
        breakpoint()
    df_d = pd.read_sql(query_orphan_datasources(), dbConnection)
    df_d["id"] = df_d["id"].astype(str)
    df_d = df_d[df_d["project site name"] == REST_conn.site_name]
    
    for index, row in df_d.iterrows():
        attachment_location = cleanup_item(row["id"], "data_source", REST_conn.download_data_source, REST_conn.delete_data_source if delete else None)
        setup_mail(
            item_name=row["data source name"],
            item_type="data_source",
            owner=row["owner"],
            email=row["email"],
            tableau_location=row["project path"],
            attachment_location=attachment_location      
        )
        move_file_to(attachment_location, BACKUP_LOCATION)
    
    return df_d
    
def cleanup_item(id: str, type: str, download_endpoint, delete_endpoint=None) -> str:
    if type not in ("workbook", "data_source"):
        raise NotImplementedError(f"Type {type} has not yet been implemented. in function cleanup_item.")
    
    unpackaged_extension = 'tds' if type == 'data_source' else 'twb'
    
    response = download_endpoint(id)
    if response.status_code != 200:
        err = lookup_error(f"download_{type}", response.status_code)
        raise Exception(err)
        
    file_location, item_extension = zip_item(response)
    
    if item_extension in (".tdsx", ".twbx"): # packaged tableau data source
        # extract the tds from the zip file
        with ZipFile(file_location, 'r') as zf:
            files = zf.namelist()
            for file in files:
                if file.endswith(unpackaged_extension):
                    extracted_file = zf.extract(file)
                    break # there is only one file we want to extract, the moment we have it we can stop the loop
        os.remove(file_location) # we remove the old zipfile since we will create a new one with the same name that contains only the tds/twb file
        with ZipFile(file_location, "w") as zf:
            try:
                extracted_file = re.findall(r".*\\(.*\.(?:twb|tds))", extracted_file)[0]
            except IndexError:
                breakpoint()
            zf.write(extracted_file, compress_type=ZIP_DEFLATED)
        os.remove(extracted_file) # since we created the zipfile we can now remove the tds/twb file  
    
    if delete_endpoint: # this will delete in case an endpoint is provided
        delete_endpoint(id)
    
    return file_location
    
        
def zip_item(response) -> str:
    filename = re.findall(r'filename="(.*)"', response.headers['Content-Disposition'])[0]
    file, extension = re.findall(r'(.*)(\..*)', filename)[0]
    
    if extension in (".twbx", ".tdsx"):
        with open(f"{file}.zip", "wb") as f:
            f.write(response.content)
        
    elif extension in (".twb", ".tds"):
        with open(filename, "wb") as f:
            f.write(response.content)
        with ZipFile(f"{file}.zip", 'w') as zf:
            zf.write(filename, compress_type=ZIP_DEFLATED)
        os.remove(filename)           
    return f"{file}.zip", extension
    

def main(delete: bool, production: bool):
    dbConn = setup_database_connection()
    restConn = setup_REST_connection("ts_config.json")
    
    workbooks_removed = cleanup_workbooks(dbConn, restConn, delete=delete)
    data_sources_removed = cleanup_datasouces(dbConn, restConn, delete=delete)
    try:
        combined = pd.concat([workbooks_removed, data_sources_removed])
        combined["item type"] = combined.apply(lambda row: "data source" if isinstance(row["data source name"], str) else "workbook", axis=1)
        combined["item name"] = combined.apply(lambda row: row["data source name"] if row["item type"]=="data source" else row["workbook name"], axis=1)
        combined = combined.drop(columns=["data source name", "workbook name"])
        combined.to_excel(f"{OUTPUT_LOCATION}\\{time.strftime('%Y-%m-%d-%HH-%MM-%SS')}.xlsx", index=False)
    except:
        print("There were no workbooks or datasources in need of deletion.")
    
    
    
    
if __name__ == "__main__":
    parser = ArgumentParser(description='A script that allows for automatic archiving of unused workbooks and data sources on Tableau XXX Site')
    parser.add_argument("--delete", dest="delete", action='store_true', help="default is set to delete")
    parser.add_argument("--no-delete", dest="delete", action='store_false', help="default is set to delete")
    
    parser.set_defaults(delete=True)
    args = parser.parse_args()
    
    OUTPUT_LOCATION = "output\\automatic_archiving"
    BACKUP_LOCATION = os.path.join(OUTPUT_LOCATION, "backup")
    
    # setting to root directory after imports
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    
    with warnings.catch_warnings(): # if your company has a proxy, each request will trigger an SSL warning. To keep the console clean we ignore these
        warnings.simplefilter("ignore")
        main(args.delete, args.production)