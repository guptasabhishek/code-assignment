#Merge from records from stg_dim_session to target dim_session table
MERGE dim_session T
USING 
(SELECT * FROM stg_dim_session)S
ON 
(
T.id = S.id
AND T.session_start_ts = S.session_start_ts
)
WHEN MATCHED THEN
  UPDATE SET

T.session_id = S.session_id
,T.session_start_ts = S.session_start_ts
,T.session_end_ts = S.session_end_ts
,T.session_duration_sec = S.session_duration_sec
,T.user_id = S.user_id
,T.ip = S.ip
,T.session_ts = S.session_ts
,T.load_ts = S.load_ts

WHEN NOT MATCHED THEN
  INSERT (
,id
,session_id
,session_start_ts
,session_end_ts
,session_duration_sec
,user_id
,ip
,session_ts
,load_ts
)

VALUES (
/*surrogate key for the new records inserted. Depending on the database could be ROWID, or next value based on a sequence etc.
This is a very simplified example.
*/
--id
,session_id
,session_start_ts
,session_end_ts
,session_duration_sec
,user_id
,ip
,session_ts
,load_ts
)