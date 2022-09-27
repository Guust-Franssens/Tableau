/*
Query finds orphan datasources (datasources that are not linked to any workbook)
and that are at least 120 days since the last update. Moreover you can add a 'NO_DELETE' tag to a data source to ensure
they are never returned in this query

Setting the 120 to a lower value will result in more aggressive deletion.
*/

WITH RECURSIVE recur_folder_structure AS 
(  
    SELECT s.id AS "project site id",
           s.name AS "project site name",
           s.url_namespace AS "project site url namespace",
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
           s.url_namespace AS "project site url namespace",
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
           "project site url namespace",
           "project id",
           "project name",
           STRING_AGG("root project name",'/' ORDER BY level DESC) AS "project path",
           MAX(special) AS special,
           MAX(level) AS level    
    FROM recur_folder_structure
    GROUP BY "project site id",
             "project site name",
             "project site url namespace",
             "project id",
             "project name"
), folder_structure AS 
(
    SELECT pp."project site id",
           pp."project site name",
           pp."project id",
           pp."project name",
           'TABLEAUSERVERURL/'||pp."project site url namespace"||'/projects/'||pp."project id" AS "project link",
           "root project id",
           "root project name",
           pp."project path",
           COALESCE(pp.special,0) AS "special"
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
    lower(u.name)||'@EMAILURL' as email,
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
ORDER BY 2,3,4