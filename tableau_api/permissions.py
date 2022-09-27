try:
    import os
    import warnings
    import contextlib
    import pandas as pd
    from utils import FlattenedDataFrame, ProjectDataFrame, ItemDataFrame, setup_REST_connection, move_to_historiek
    from tableau_api_lib.utils import extract_pages
except ModuleNotFoundError as e:
    print("\n\n--------------------------------------------------------------------------------------------------------------\n")
    print("The python environment you are running this script in does not contain all the dependencies.")
    print("Please look at the 'update environment' section in the 'werkinstructie' how to update the environment")
    print("\n--------------------------------------------------------------------------------------------------------------\n\n")
    raise Exception(str(e)) from None

TS_CONFIG_NAME = "ts_config.json"

# mapping was retrieved from: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_permissions.htm
PERMISSION_MAPPING = { 
    "AddComment": "Add Comment",
    "ChangeHierarchy": "Move",
    "ChangePermissions": "Set Permissions",
    "CreateRefreshMetrics": "Create/Refresh Metrics",
    "Execute": "Run Flow",
    "ExportData": "View Summary Data",
    "ExportImage": "Export Image",
    "ExportXml": "Download",
    "InheritedProjectLeader": "Project Leader",
    "ProjectLeader": "Project Leader",
    "Read": "View",
    "RunExplainData": "Run Explain Data",
    "ShareView": "Share Customized",
    "SaveAs": "Save As",
    "ViewComments": "View Comments",
    "ViewUnderlyingData": "View Underlying Data",
    "WebAuthoring": "Web Edit",
    "Write": "Save"
}

def project_permissions(conn, df, df_projects):
    project_responses = []
    
    # we only need to get the permissions for the root projects and subfolders that are not locked by the root folder
    mask = df_projects["parentProjectId"].isna() | ~ (df_projects["rootParentContentPermissions"] =="LockedToProject")
    df_projects = df_projects.loc[mask, :]
    
    for _, row in df_projects.iterrows():
        id, name, rootParentProjectId = row["id"], row["name"], row["rootParentProjectId"]
        # with statement blocks print statements done by FlattenedDataFrame
        with open(os.devnull, "w") as f, contextlib.redirect_stdout(f):
            temp = FlattenedDataFrame(conn.query_project_permissions(id).json().get("permissions").get("granteeCapabilities"))
        temp[["item_id", "item_name", "item_type", "item_project", "rootParentProjectId"]] = id, name, "project", name, rootParentProjectId
        project_responses.append(temp)
    
    to_return =  df.merge(
        pd.concat(project_responses).reset_index(drop=True).drop(columns="snapshot_date"),
        how="left", 
        left_on="id", 
        right_on="rootParentProjectId",
        suffixes=["", "_y"]
    ).drop(columns="rootParentProjectId_y")
    
    return to_return
   
def workbook_permissions(conn, df, df_workbooks):
    workbook_responses = []

    for _, row in df_workbooks.iterrows():
        id, name, project_name, rootParentProjectId = row["id"], row["name"], row["project_name"], row["rootParentProjectId"]
        
        # with statement blocks the unnecessary prints done by FlattenedDataFrame
        with open(os.devnull, "w") as f, contextlib.redirect_stdout(f):
            temp = FlattenedDataFrame(conn.query_workbook_permissions(id).json().get("permissions").get("granteeCapabilities"))
        temp[["item_id", "item_name", "item_type", "item_project", "rootParentProjectId"]] = id, name, "workbook", project_name, rootParentProjectId
        workbook_responses.append(temp)
    
    to_return =  df.merge(
        pd.concat(workbook_responses).reset_index(drop=True).drop(columns="snapshot_date"),
        how="inner", 
        left_on="id", 
        right_on="rootParentProjectId",
        suffixes=["", "_y"]
    ).drop(columns="rootParentProjectId_y")
    
    return to_return
    
def datasource_permissions(conn, df, df_datasources):
    datasource_responses = []
    
    for _, row in df_datasources.iterrows():
        id, name, project_name, rootParentProjectId = row["id"], row["name"], row["project_name"], row["rootParentProjectId"]
        
        # with statement blocks the unnecessary prints done by FlattenedDataFrame
        with open(os.devnull, "w") as f, contextlib.redirect_stdout(f):
            temp = FlattenedDataFrame(conn.query_data_source_permissions(id).json().get("permissions").get("granteeCapabilities"))
        temp[["item_id", "item_name", "item_type", "item_project", "rootParentProjectId"]] = id, name, "datasource", project_name, rootParentProjectId
        datasource_responses.append(temp)
    
    to_return =  df.merge(
        pd.concat(datasource_responses).reset_index(drop=True).drop(columns="snapshot_date"),
        how="inner", 
        left_on="id", 
        right_on="rootParentProjectId",
        suffixes=["", "_y"]
    ).drop(columns="rootParentProjectId_y")
    
    return to_return

def flow_permissions(conn, df, df_flows):
    flow_responses = []
    
    for _, row in df_flows.iterrows():
        id, name, project_name, rootParentProjectId = row["id"], row["name"], row["project_name"], row["rootParentProjectId"]
        
        # with statement blocks the unnecessary prints done by FlattenedDataFrame
        with open(os.devnull, "w") as f, contextlib.redirect_stdout(f):
            temp = FlattenedDataFrame(conn.query_flow_permissions(id).json().get("permissions").get("granteeCapabilities"))
        temp[["item_id", "item_name", "item_type", "item_project", "rootParentProjectId"]] = id, name, "flow", project_name, rootParentProjectId
        flow_responses.append(temp)
        
    to_return =  df.merge(
        pd.concat(flow_responses).reset_index(drop=True).drop(columns="snapshot_date"),
        how="inner", 
        left_on="id", 
        right_on="rootParentProjectId",
        suffixes=["", "_y"]
    ).drop(columns="rootParentProjectId_y")
    
    return to_return

def main():
    conn = setup_REST_connection(TS_CONFIG_NAME)
    
    # to have a full view of the permissions we need the permissions of all projects, data sources, workbooks, flows and combine this with the groups and users
    # with statement blocks the unnecessary prints done by FlattenedDataFrame
    with open(os.devnull, "w") as f, contextlib.redirect_stdout(f):
        df_projects = ProjectDataFrame(extract_pages(conn.query_projects)) 
        df_datasources = ItemDataFrame(extract_pages(conn.query_data_sources), df_projects) # we pass df_projects so we can see in which root project the data source lies
        df_workbooks = ItemDataFrame(extract_pages(conn.query_workbooks_for_site), df_projects) # we pass df_projects so we can see in which root project the workbook lies
        df_flows = ItemDataFrame(extract_pages(conn.query_flows_for_site), df_projects) # we pass df_projects so we can see in which root project the flow lies
        df_groups = FlattenedDataFrame(extract_pages(conn.query_groups))[["id", "name"]].rename(columns={"id":"group_id", "name": "group_name"})
        df_users = FlattenedDataFrame(extract_pages(conn.get_users_on_site))[["id", "name"]].rename(columns={"id": "user_id", "name":"user_name"})
    
    # we will use the root project as our main dataframe to which we will join the projects, data sources and workbooks 
    df = df_projects.loc[df_projects["parentProjectId"].isna()].reset_index(drop=True)
    
    # retrieving project, datasource, workbook and flow permissions, then combining them into one dataframe
    print("Retrieving project, datasource, workbook and flow permissions. This might take a while...")
    pp = project_permissions(conn, df, df_projects)
    print("\tProject permissions done!")
    dp = datasource_permissions(conn, df, df_datasources)
    print("\tData Source permissions done!")
    wp = workbook_permissions(conn, df, df_workbooks)
    print("\tWorkbook permissions done!")
    fp = flow_permissions(conn,df, df_flows)
    print("\tFlow permissions done!")
    print("Done!\n")
    
    print("Combining permissions...", end="")
    df = pd.concat([pp, dp, wp, fp]).reset_index(drop=True)
    print(" Done!\n")
    
    
    # adding groupnames
    print("Adding group names...", end="")
    df = df.merge(df_groups, how="left", left_on="group_id", right_on="group_id")
    print(" Done!\n")
    
    # adding users
    print("Adding user names...", end="")
    df = df.merge(df_users, how="left", left_on="user_id", right_on="user_id")
    print(" Done!\n")
    
    # tidying up the dataframe
    print("Reformatting the data...", end="")
    df.loc[df["group_id"].notna(), "grantee_type"] = "group"
    df.loc[df["group_id"].notna(), "grantee_id"] = df.loc[df["group_id"].notna(), "group_id"]
    df.loc[df["group_id"].notna(), "grantee"] = df.loc[df["group_id"].notna(), "group_name"]
    df.loc[df["user_id"].notna(), "grantee_type"] = "user"
    df.loc[df["user_id"].notna(), "grantee_id"] = df.loc[df["user_id"].notna(), "user_id"]
    df.loc[df["user_id"].notna(), "grantee"] = df.loc[df["user_id"].notna(), "user_name"]
    df = df.drop(columns=["parentProjectId", "group_id", "user_id", "group_name", "user_name"])
    df = df.rename(
        columns={
        "name": "root_parent_project_name",
        "rootParentProjectId": "root_parent_project_id",
        "rootParentContentPermissions": "root_parent_content_permissions"}
    )
    df["capabilities_capability_name"] = df.apply(lambda row: PERMISSION_MAPPING.get(row["capabilities_capability_name"], row["capabilities_capability_name"]), axis=1)
    print(" Done!\n")
    
    move_to_historiek("output\\permissions")
    
    # saving to output folder and to gateway
    print("Saving results...", end="")
    df.to_excel("output\\permissions\\TAB_CDAO_Tableau_permissions.xlsx", sheet_name="Tableau_permissions", index=False)
    print(" Done!")
    
    
if __name__ == "__main__":
    # setting to root directory after imports
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    
    with warnings.catch_warnings(): # we get a warning that no ssl is inplace. We ignore this warning since this is due to the companies proxy
        warnings.simplefilter("ignore")
        main()




