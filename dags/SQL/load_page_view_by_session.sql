#Merge from records from stg_page_view_by_session to target fact page_view_by_session table
MERGE page_view_by_session T
USING 
(SELECT * FROM stg_page_view_by_session)S
ON 
(
T.session_id = S.session_id
AND T.content_id = S.content_id
AND T.page_id = S.page_id
AND T.referrer_page_id = S.referrer_page_id
AND T.date_id = S.date_id
)
WHEN MATCHED THEN
  UPDATE SET

T.page_view_order = S.page_view_order
,T.page_view_start_ts = S.page_view_start_ts
,T.page_view_end_ts = S.page_view_end_ts
,T.session_duration_sec = S.session_duration_sec
,T.page_view_duration_sec = S.page_view_duration_sec
,T.page_count = S.page_count
,T.session_ts = S.session_ts
,T.content_count = S.content_count
,T.load_ts = S.load_ts

WHEN NOT MATCHED THEN
  INSERT (

session_id
,page_id
,referrer_page_id
,page_view_order
,page_view_start_ts
,page_view_end_ts
,page_view_duration_sec
,content_id
,date_id
,page_count
,content_count
,load_ts
)

VALUES (

session_id
,page_id
,referrer_page_id
,page_view_order
,page_view_start_ts
,page_view_end_ts
,page_view_duration_sec
,content_id
,date_id
,page_count
,content_count
,load_ts
)