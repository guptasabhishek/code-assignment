/* Populate content dim, read from content table identify new records and insert into stg_dim_content*/
/* I am assuming that new content can be created daliy, or existing content can be udpated daily*/
/* I dont fully understand the use of version column. I am assuming this indicates new version of content.  */
/* I am going with the assumption that I will updated the target table with the current version, e.g SCD Type 1. */

SELECT
ISNULL(dim_content.id, 0)
,stg_dim_content.contentId
,stg_dim_content.contentType
,stg_dim_content.creationDate
,stg_dim_content.version
,stg_dim_content.body
,stg_dim_content.author
,stg_dim_content.relatedMedia
,stg_dim_content.load_ts
FROM
(
SELECT
contentId
,contentType
,creationDate
,version
,body
,author
,relatedMedia
,CURRENT_TIMESTAMP AS load_ts
,ROW_NUMBER() OVER(PARTITION BY contentId,version ORDER BY creationDate desc) AS current_content_record
FROM
content
WHERE 
1=1
#Access content table based on a partion available DATE(creationDate) column.
AND DATE(creationDate) = CURRENT_DATE
# Retereive records updated with the past 2 hours
AND creationDate >= TIMESTAMP_ADD(CURRENT_TIMESTAMP, INTERVAL -2 HOUR))
) stg_dim_content
LEFT JOIN dim_content ON 
stg_dim_content.contentId = dim_content.contentId

WHERE
stg_dim_content.current_content_record = 1


