/*
This query returns all projects, and under which "root" project they lie. 
You can adapt the "project link" so that you have an URL that points to the project. 

You can combine this query with for example workbooks to find out under which root project the workbook is located.
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
)
SELECT *
FROM folder_structure
ORDER BY 1,2,4;