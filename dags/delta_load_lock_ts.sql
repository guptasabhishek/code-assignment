#To insert populate windowing table values before new job starts

INSERT INTO pageview_deltaload_window_log
(
unlock_ts,
lock_ts,
job_begin_ts, 
job_end_ts
)

SELECT 
#Set window start time, this is the end timestamp value from the last successful load.
(SELECT MAX(lock_ts) FROM pageview_deltaload_window_log WHERE job_end_ts IS NOT NULL), 
#Set window end time, this is max timestamp from the source to set the end value for current load window.
(SELECT MAX(timestamp) FROM pageview WHERE DATE(pageview) = CURRENT_DATE), 
NULL,
NULL,
CURRENT_TIMESTAMP, 
NULL

