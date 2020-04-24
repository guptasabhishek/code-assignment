/* Populate stg_page_view_by_session table, read from stg_session_pageview table identify new records and insert into stg_dim_session*/

WITH stg_pageview_session_cte1 AS (
SELECT DISTINCT
userId
,contentId
,pageUrl
,sessionId
,ip
,timestamp
,referrerUrl
FROM
stg_session_pageview
)

,stg_pageview_session_cte2 AS (
SELECT
stg_pageview_session.session_id AS session_id
,stg_pageview_session.pageUrl AS pageUrl
,stg_pageview_session.referrerUrl AS referrerUrl
,ROW_NUMBER() OVER (PARTITION BY stg_pageview_session.session_id,stg_pageview_session.userId,stg_pageview_session.ip,stg_pageview_session.pageUrl ORDER BY timestamp asc) AS page_view_order
,timestamp AS page_view_start_ts
,LEAD(timestamp) OVER (PARTITION BY stg_pageview_session.session_id,stg_pageview_session.userId,stg_pageview_session.ip,stg_pageview_session.pageUrl ORDER BY timestamp asc) AS page_view_end_ts
,stg_pageview_session.content_id AS content_id
,FORMAT(CAST(timestamp as DATE),'yyyyMMdd') as date_key
,1 AS page_count
,1 AS content_count
,stg_session.load_ts
FROM
stg_pageview_session_cte1
)

SELECT

dim_session.id AS session_id
,page_url.id AS page_id
,referrer_url.id AS referrer_page_id
,stg_pageview_session_cte2.page_view_order AS page_view_order
,stg_pageview_session_cte2.page_view_start_ts AS page_view_start_ts
,stg_pageview_session_cte2.page_view_end_ts AS page_view_end_ts
,((((stg_pageview_session_cte2.page_view_start_ts-stg_pageview_session_cte2.page_view_end_ts)*24)*60)*60) AS page_view_duration_sec
,dim_content.id AS content_id
,dim_date.id AS date_id
,stg_pageview_session_cte2.page_count AS page_count
,stg_pageview_session_cte2.content_count AS content_count
,stg_pageview_session_cte2.load_ts AS load_ts
FROM
stg_pageview_session_cte2

LEFT JOIN dim_session ON 
stg_pageview_session_cte2.session_id = dim_session.session_id
AND stg_pageview_session_cte2.user_id = dim_session.user_id
AND stg_pageview_session_cte2.ip = dim_session.ip
AND stg_pageview_session_cte2.timestamp = dim_session.timestamp

LEFT JOIN dim_content ON
stg_pageview_session_cte2.contentId = dim_content.contentId

LEFT JOIN dim_page page_url ON 
stg_pageview_session_cte2.url = page_url.url

LEFT JOIN dim_page referrer_url ON 
stg_pageview_session_cte2.url = referrer_url.url

LEFT JOIN dim_date ON
stg_pageview_session_cte2.date_key = dim_date.date

