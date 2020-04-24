#Merge from records from stg_dim_content to target dim_content table
MERGE dim_content T
USING 
(SELECT * FROM stg_dim_content)S
ON 
(
T.id = S.id
)
WHEN MATCHED THEN
  UPDATE SET

T.contentId = S.contentId
,T.contentType = S.contentType
,T.creationDate = S.creationDate
,T.version = S.version
,T.body = S.body
,T.author = S.author
,T.relatedMedia = S.relatedMedia
,T.load_ts = S.load_ts

WHEN NOT MATCHED THEN
  INSERT (
id
,contentId
,contentType
,creationDate
,version
,body
,author
,relatedMedia
,load_ts
)

VALUES (
/*surrogate key for the new records inserted. Depending on the database could be ROWID, or next value based on a sequence etc.
This is a very simplified example.
*/
--id
,contentId
,contentType
,creationDate
,version
,body
,author
,relatedMedia
,load_ts
)