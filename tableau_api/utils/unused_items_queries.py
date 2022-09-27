__all__ = ['query_orphan_datasources', 'query_unused_workbooks']
ORGANISATION_EMAIL_SUFFIX = "@ORGNAME.DOMAIN"

def query_orphan_datasources() -> str: 
    query = (
        fr"""
        WITH RECURSIVE recur_folder_structure AS 
        (  
            SELECT s.id AS "project site id",
                   s.name AS "project site name",
                   p.id AS "project id",
                   p.name AS "project name",
                   p.id AS "root project id",
                   p.name AS "root project name",
                   p.special AS "special",
                   1 AS level
            FROM public.projects p
            JOIN public.sites s on p.site_id = s.id
        UNION
            SELECT s.id AS "project site id",
                   s.name AS "project site name",
                   p.id AS "project id",
                   p.name AS "project name",
                   COALESCE(recur_folder_structure."root project id", p.id) AS "root project id",
                   COALESCE(recur_folder_structure."root project name", p.name) AS "root project name",
                   p."special",
                   recur_folder_structure.level + 1 AS level
            FROM public.projects p
            JOIN public.sites s on p.site_id = s.id
            JOIN recur_folder_structure ON p.parent_project_id = recur_folder_structure."project id"
        ), project_path AS
        (
            SELECT "project site id",
                   "project site name",
                   "project id",
                   "project name",
                   STRING_AGG("root project name",'/' ORDER BY level DESC) AS "project path",
                   MAX(special) AS special,
                   MAX(level) AS level    
            FROM recur_folder_structure
            GROUP BY "project site id",
                     "project site name",
                     "project id",
                     "project name"
        ), folder_structure AS 
        (
            SELECT pp."project site id",
                   pp."project site name",
                   pp."project id",
                   pp."project name",
                   "root project id",
                   "root project name",
                   pp."project path",
            FROM project_path pp
            JOIN recur_folder_structure rfs
            ON pp."project site id" = rfs."project site id"
            AND pp."project site name" = rfs."project site name"
            AND pp."project id" = rfs."project id"
            AND pp."project name" = rfs."project name"
            AND pp.level = rfs.level
        ), NO_DELETE AS
        (
        SELECT DISTINCT
            COALESCE(v.workbook_id, tt.taggable_id)         AS tag_item_id,
            CASE tt.taggable_type                               
                WHEN 'View' THEN 'Workbook'
                ELSE tt.taggable_type
            END                                             AS tag_item_type,
            t.name                                          AS tag_value
        FROM tags t
        JOIN taggings tt on t.id = tt.tag_id
        LEFT JOIN views v on tt.taggable_id = v.id AND tt.taggable_type = 'View'
        WHERE t.name = 'NO_DELETE'
        )
        SELECT 
            ds.luid as id,
            fs."project site name", 
            fs."project path",
            ds.name as "data source name", 
            u.friendly_name as owner,
            lower(u.name)||'{ORGANISATION_EMAIL_SUFFIX}' as email,
            ds.last_published_at
        FROM datasources ds
        JOIN folder_structure fs ON ds.project_id = fs."project id"
        LEFT JOIN _users u ON ds.owner_id = u.id
        LEFT JOIN data_connections dc ON ds.repository_url = dc.dbname
        LEFT JOIN NO_DELETE nd ON nd.tag_item_id = ds.id AND nd.tag_item_type = 'Datasource'
        WHERE ds.parent_type is null
        AND nd.tag_value IS NULL
        AND dc.id is null
        AND ds.last_published_at <= NOW() - INTERVAL '120 days' 
        """
    )
    return query
    
def query_unused_workbooks() -> str:
    query = (
        fr"""
        WITH RECURSIVE recur_folder_structure AS 
        (  
            SELECT s.id AS "project site id",
                   s.name AS "project site name",
                   p.id AS "project id",
                   p.name AS "project name",
                   p.id AS "root project id",
                   p.name AS "root project name",
                   p.special AS "special",
                   1 AS level
            FROM public.projects p
            JOIN public.sites s on p.site_id = s.id
        UNION
            SELECT s.id AS "project site id",
                   s.name AS "project site name",
                   p.id AS "project id",
                   p.name AS "project name",
                   COALESCE(recur_folder_structure."root project id", p.id) AS "root project id",
                   COALESCE(recur_folder_structure."root project name", p.name) AS "root project name",
                   p."special",
                   recur_folder_structure.level + 1 AS level
            FROM public.projects p
            JOIN public.sites s on p.site_id = s.id
            JOIN recur_folder_structure ON p.parent_project_id = recur_folder_structure."project id"
        ), project_path AS
        (
            SELECT "project site id",
                   "project site name",
                   "project id",
                   "project name",
                   STRING_AGG("root project name",'/' ORDER BY level DESC) AS "project path",
                   MAX(special) AS special,
                   MAX(level) AS level    
            FROM recur_folder_structure
            GROUP BY "project site id",
                     "project site name",
                     "project id",
                     "project name"
        ), folder_structure AS 
        (
            SELECT pp."project site id",
                   pp."project site name",
                   pp."project id",
                   pp."project name",
                   "root project id",
                   "root project name",
                   pp."project path",
            FROM project_path pp
            JOIN recur_folder_structure rfs
            ON pp."project site id" = rfs."project site id"
            AND pp."project site name" = rfs."project site name"
            AND pp."project id" = rfs."project id"
            AND pp."project name" = rfs."project name"
            AND pp.level = rfs.level
        ), NO_DELETE AS
        (
            SELECT DISTINCT
                COALESCE(v.workbook_id, tt.taggable_id)         AS tag_item_id,
                CASE tt.taggable_type                               
                    WHEN 'View' THEN 'Workbook'
                    ELSE tt.taggable_type
                END                                             AS tag_item_type,
                t.name                                          AS tag_value
            FROM tags t
            JOIN taggings tt on t.id = tt.tag_id
            LEFT JOIN views v on tt.taggable_id = v.id AND tt.taggable_type = 'View'
            WHERE t.name = 'NO_DELETE'
        )   
        SELECT 
            w.luid as id, 
            fs."project site name", 
            fs."project path", 
            w.name as "workbook name", 
            u.friendly_name as owner,
            lower(u.name)||'{ORGANISATION_EMAIL_SUFFIX}' as email,
            pa."project lead emails",
            MAX(he.created_at) AS "last used passed six months"
        FROM workbooks w 
        JOIN folder_structure fs ON fs."project id" = w.project_id
        LEFT JOIN hist_workbooks hw ON hw.workbook_id = w.id
        LEFT JOIN historical_events he ON hw.id = he.hist_workbook_id
        LEFT JOIN _users u ON w.owner_id = u.id
        LEFT JOIN NO_DELETE nd ON nd.tag_item_id = w.id and nd.tag_item_type = 'Workbook'
        WHERE  
        w.created_at <= NOW() - INTERVAL '30 days' -- to not delete just created dashboards 
        AND nd.tag_value IS NULL
        GROUP BY fs."project site name", fs."project path", w.luid, w.name, u.friendly_name, u.name
        HAVING (MAX(he.created_at) IS NULL OR MAX(he.created_at) <= NOW() - INTERVAL '120 days')
        """
    )
    return query
    
