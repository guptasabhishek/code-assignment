/* This script retreives all new records from the source pageview table that 
have been inserted into the table since the last successfull session_pageview run*/

SELECT
userId
,contentId
,pageUrl
,sessionId
,ip
,timestamp
,referrerUrl

FROM
pageview
WHERE
1=1
#Access pageview table based on a partion available DATE(timestamp) column.
AND DATE(timestamp) = CURRENT_DATE 
#Once we set our window start and end, we only want to reterive rows from source tha fall within that window. 
#Considering we are delta loading,this is done to aviod reprocessing old records.
AND timestamp between (SELECT unlock_ts FROM pageview_deltaload_window_log WHERE job_end_ts IS NULL) 
AND (SELECT MAX(lock_ts) FROM pageview_deltaload_window_log WHERE job_end_ts IS NULL)