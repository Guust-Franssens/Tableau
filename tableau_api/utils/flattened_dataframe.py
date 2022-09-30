import pandas as pd
import warnings
import time
import numpy as np

class FlattenedDataFrame(pd.DataFrame):
    def __init__(self, data=None, index=None, columns=None, dtype=None, copy=None):
        # initializing DataFrame class
        super().__init__(data=data, index=index, columns=columns, dtype=dtype, copy=copy)
        
        # pandas raises a warning that we should not set columns directly (we are only accessing so no problem)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.original_columns = self.columns
        
        self.flatten()
        self["snapshot_date"] = pd.to_datetime('today').strftime('%Y-%m-%d')
    
    def flatten(self):
        flattened_columns = {}
        while self.check_dtypes(): # as long as there are columns with type dict or list keep unpacking
            for columnname, typename in self.get_dtypes().items():
                if typename in ('str', 'int', 'float', 'NoneType', 'bool', 'numpy.bool_', 'bool_', 'int64', 'Series', 'NoneType'):
                    continue

                elif typename == 'dict':
                    flattened_columns[columnname] = "dict"
                    temp = pd.concat([self.loc[:, self.columns != columnname], self._flatten_dict(columnname)], axis=1)
                    self.re_inititialize_super_class(temp)
                    break  # since we unpacked a column we need to restart the for loop

                elif typename == 'list':
                    flattened_columns[columnname] = "list"
                    temp = self.loc[:, self.columns != columnname].join(self._flatten_list(columnname), how="left")
                    self.re_inititialize_super_class(temp)
                    break  # since we unpacked a column we need to restart the for loop

                else:
                    print(typename=='NoneType')
                    msg = (
                        f"flattening type: {typename} found in column {columnname}has not been implemented\n"
                    )
                    raise NotImplementedError(msg)
                
        newly_added_columns = [column for column in self.columns if column not in self.original_columns]

        if newly_added_columns:
            print(f"By unpacking {len(newly_added_columns)} new columns were added:\n{newly_added_columns}\n")
        else:
            print(f"Data did not require unpacking, no new columns were added.\n")

        return self
                
    def re_inititialize_super_class(self, new_df):
        super().__init__(data=new_df.values, columns=new_df.columns) 

    def get_dtypes(self) -> dict: 
        """
        Checks in depth which datatype a column contains. 
        Instead of returning "object" for a column, this function will explain whether it is a str, list, dict ...
        """
        result = {}
        
        # pandas raises a warning that we should not set columns directly (we are only accessing so no problem)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            for columnname in self.columns:
                index = self.loc[:, columnname].first_valid_index()
                if index is not None:
                    value = self.loc[index, columnname]
                    result[columnname] = type(value).__name__
                else:
                    result[columnname] = type(None).__name__
        return result
    
    def check_dtypes(self):
        dtypes = self.get_dtypes().values()
        return ('list' in dtypes or 'dict' in dtypes)
    
    def _flatten_dict(self, columnname) -> pd.DataFrame:
        # checking if there are any NaN values, if there are we have to do transform them from NaN to {key: NaN}
        if self[columnname].isnull().values.any():
            # dirty way of extracting the key from the key: value in the dictionary
            key = list(self.loc[self.loc[:, columnname].first_valid_index(), columnname].keys())[0]
            # replacing NaN with {key: NaN}
            self.loc[self[columnname].isna(), columnname] = self.loc[self[columnname].isna(), columnname].apply(lambda x: {key: np.nan})
        replacement = pd.DataFrame(self.loc[:, columnname].tolist())
        replacement = replacement.add_prefix(f"{columnname}_")
        return replacement

    def _flatten_list(self, columnname) -> pd.DataFrame:
        values = self.loc[:, columnname].to_dict()
        list_dfs = []
        for index, list_dict in values.items():
            if not isinstance(list_dict, list):  # nan values are not considered a list, however we can skip them as they dont require unpacking
                continue
            replacement = pd.DataFrame(list_dict, index=[index]*len(list_dict))
            replacement = replacement.add_prefix(f"{columnname}_")
            list_dfs.append(replacement)
        return pd.concat(list_dfs)

class ProjectDataFrame(FlattenedDataFrame):
    COLUMNS_TO_KEEP = ["snapshot_date", "id", "name", "parentProjectId", "rootParentProjectId", "rootParentContentPermissions"]
    def __init__(self, data=None, index=None, columns=None, dtype=None, copy=None):
        super().__init__(data=data, index=index, columns=columns, dtype=dtype, copy=copy)
        self.set_index("id", drop=False, inplace=True)
        self["rootParentProjectId"] = self.apply(lambda row: self.find_root_project(row['id']), axis=1)
        super().re_inititialize_super_class(self.find_root_content_permissions())
        super().re_inititialize_super_class(self[self.COLUMNS_TO_KEEP])
    
    def find_root_project(self, project_id: str) -> str:
        parent_id = self.at[project_id, 'parentProjectId']
        
        # if there is no parent project then we are already at the root project
        if isinstance(parent_id, float):
            return project_id
        
        # otherwise recursively call find_root_project with the parent_id until we are at the root project
        else:
            return self.find_root_project(parent_id)
        
    def find_root_content_permissions(self):
        root_projects = self.loc[self["parentProjectId"].isna(), ["rootParentProjectId", "contentPermissions"]]
        root_projects = root_projects.rename(columns={"contentPermissions":"rootParentContentPermissions"})
        merged = self.merge(root_projects, how="inner", on="rootParentProjectId").reset_index(drop=True)
        return merged.set_index("id", drop=False)
        
            
class ItemDataFrame(FlattenedDataFrame):
    def __init__(self, data, project_dataframe, index=None, columns=None, dtype=None, copy=None):
        super().__init__(data=data, index=index, columns=columns, dtype=dtype, copy=copy)
        self._project_dataframe = project_dataframe.set_index("id", drop=False)
         
        self["rootParentProjectId"] = self.apply(lambda row: self.safe_at(row["project_id"]), axis=1)
        super().re_inititialize_super_class(self.loc[self["rootParentProjectId"].notna(), :])
    
    def safe_at(self, project_id: str):
        try:
            return self._project_dataframe.at[project_id, "rootParentProjectId"]
        except KeyError:
            return None
