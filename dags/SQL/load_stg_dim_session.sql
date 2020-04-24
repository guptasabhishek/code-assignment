/* Populate session dim stage table, read from stg_session_pageview table identify new records and insert into stg_dim_session*/

WITH stg_session AS (
SELECT DISTINCT
userId
,sessionId
,ip
,timestamp
,current_timestamp as load_ts
FROM
stg_session_pageview
)

, stg_session_l2 AS (
SELECT
stg_session.session_id AS session_id
,stg_session.timestamp AS session_start_ts
,stg_session.userId AS user_id
,stg_session.ip AS ip
,stg_session.timestamp AS session_ts
,MAX(stg_session.timestamp) OVER (PARTITION BY stg_session.session_id, stg_session.userId, stg_session.ip) AS session_end_ts
,stg_session.load_ts
FROM
stg_session
)

SELECT
ISNULL(dim_session.id, 0)
,stg_session_l2.session_id AS session_id
,stg_session_l2.session_start_ts AS session_start_ts
,stg_session_l2.session_end_ts AS session_end_ts
,((((stg_session_l2.session_end_ts - stg_session_l2.session_start_ts)*24)*60)*60) AS session_duration_sec
,stg_session_l2.userId AS user_id
,stg_session_l2.ip AS ip
,stg_session_l2.timestamp AS session_ts
,stg_session_l2.load_ts
FROM
stg_session_l2
LEFT JOIN dim_session ON 
stg_session_l2.session_id = dim_session.session_id
AND stg_session_l2.user_id = dim_session.user_id
AND stg_session_l2.ip = dim_session.ip
AND stg_session_l2.timestamp = dim_session.timestamp

