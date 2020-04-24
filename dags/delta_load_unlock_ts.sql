#To update populate windowing table values after current job ends

UPDATE pageview_deltaload_window_log
SET 
#Update job_end_ts for current load
job_end_ts = CURRENT_TIMESTAMP

WHERE 
#Identify current load, 
job_begin_ts = (SELECT MAX(job_begin_ts) FROM pageview_deltaload_window_log WHERE job_end_ts IS NULL)
