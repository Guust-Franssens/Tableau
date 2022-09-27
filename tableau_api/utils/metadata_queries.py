queries = {
    "DatabaseServer": """
        {
            databaseServers {
                id  
                luid
                name
                hostName
                isEmbedded
                port
                connectionType
                isEmbedded     
            
                tables {
                    id
                    name
                    schema
                    fullName
                    isEmbedded
                }    
            }
        }""",
    
    "files": """
        {
            files {
                id
                luid
                name
                isEmbedded
                filePath
                __typename
                connectionType
                tables {
                    id
                    name
                } 
            }
        }""",
    
    "Databases": """
        {
            databases{
                id
                luid
                name
                __typename
                connectionType
            }
        }
    """,
    
    "Tables": """
        {
            tables{
                id
                name
                isEmbedded
            }
        }
    """,
    
    "databasetable": """
        {
            databaseTables{
                id
                luid
                name
                isEmbedded
                schema
                fullName
            }
        }
    """,
    
    "customsqltables": """
        {
            customSQLTables{
                id
                name
                isEmbedded
                query
            }
        }
    """,
    
    "EmbeddedDatasources": """
        {
            embeddedDatasources{
                id
                name
                hasExtracts
                extractLastRefreshTime
                extractLastUpdateTime
                extractLastIncrementalUpdateTime
                extractLastUpdateTime
                
                workbook {
                    id
                    name
                    projectName
                }
                
                upstreamTables {
                    id
                    name
                }
            }
        }
    """,
    
    "publisheddatasources": """
        {
            publishedDatasources {
                id
                name
                projectName
                
                site {
                    id
                    name
                }
                
                owner {
                    id
                    name
                }
                
                hasExtracts
                extractLastRefreshTime
                extractLastUpdateTime
                extractLastIncrementalUpdateTime
                extractLastUpdateTime
                
                downstreamWorkbooks {
                    name
                    projectName
                }
                
                upstreamTables {
                    id
                    name
                    schema
                    fullName
                    database {
                        name
                    }
                }
                
                downstreamTables {
                    id
                    name
                }
            }
        }
    """,
    
    "Disconnecteddatasources": """
        {
            workbooks {
                name
                projectName
                embeddedDatasources {
                    name
                    upstreamDatasources {
                        name
                    }
                    upstreamDatabases {
                        name
                    }
                }  
            }
        }
    """,
    
    "users": """
        {
            tableauUsers{
                id
                name
                username
                domain
                email
                
                ownedWorkbooks {
                    id
                    name
                    projectName
                }
            }
        }
    """,
    
    "workbooks": """
        {
            workbooks {
                id
                name
                projectName
                
                dashboards {
                    id
                    name
                    sheets {
                        id
                        name
                        
                        datasourceFields {
                            id
                            name
                            upstreamTables {
                                id
                            }
                        }
                    }
                }
            }
        }
    """
}