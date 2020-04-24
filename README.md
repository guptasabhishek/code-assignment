# code-assignment 

code-assignment: PageviewCMS_POOL
 
Assumptions: 

1. I am designing the underlying physical data model (facts, dims) based on the two tables (pageviews, and content). Product owner will     then design the datasets/metadata layer in Tableau, Looker etc, utilizing these fact and dim tables, addressing each of their use-        cases. Initial full load, followed by incremental loads. 

2. pageview table is partitioned by date(timestamp) for column timestamp 
  **recommendation** can this column name be changed to some other name like activtyTimestamp etc, considering timestamp is a reserved      keyword in most SQL implementations.  
 
3. content table is partitioned by date(creationDate) 

Questions: 

Thank you Alex for answering these!!!!! 

1. How often is data loaded into pageview and content tables? Realtime/batch? 

--Let’s assume the data is loaded in near real-time, a batch every 5 mins. 

2. How often do the fact and dim tables need to be updated? Daily, Hourly, etc. 

--For the data freshness, let’s set 1 hr as our target 

3. Are these source tables partitioned, to avoid full table scans while accessing them. 

--For partitions, let’s say that currently there is no partitioning on any of these tables… could you provide a suggestion on how you might partition the tables to optimize for this use case. 

4. Content table has a version column. Is a new record inserted into the content table with an incremental version number? i.e Can rows duplicate for each contentId, but the grain is at the contentId + version level? Or existing record updated with a new version number? 
For this slowly chaining dim, is there an SCD requirement (1,2,3)? 
Or for this exercise should I assume there is always only one record per contentID in the content table. 

--The version expands contentType by describing which version “schema” was used to create the specific piece of content. contentId is the natural key for this table. 

5. For the average user session duration trends requirement, I see sessionId on the pageviews table, but no additional metadata related to sessionId. 

--User sessions are managed on the client side, so when a record comes in the sessionId already exists. With the assumption that the client will always send you the valid sessionId for the user, you would just need to ensure this value can be used efficiently as a dimension. 

6. I am a bit confused between pageUrl and referrerUrl. Is referreURL the parent page from when the user navigated to the current page, and so forth? 

--Yes, in the web context there is a header value that the browser provides that explains where the user was before this page. Here’s a --link for reference https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Referer 

7. Can sessionId repeat for same user, ip, but have unique values for pageUrl, RefferUrl, timestamp, contentId? 

--It could repeat on user, ip, pageUrl, RefferUrl, and contentId; but would be unique on timestamp... as an example the user might go to --the homepage many times in a single session.
  

########## 
#SOLUTION# 
########## 
  

Model related information is stored in the code-assignment/model folder 

for this exercise I created a dimensional model consisting of: 

dim_session 

page_view_by_session (fact) 

dim_page 

dim_content 

dim_date (I assumed a date dim already exists) 

Most of my Airflow experience has been utilizing the BigQuery operator. For this specific use case I have created the using the BigQuery operator. 

I have created 1 DAG (logical flow) that includes 10 tasks, for simplicity they are all running sequentially. 1 >> 2 >> 3 >>......>> 10. 
Scheduled to run hourly via airflow schedule. 

sessions_pageview.py dag config is stored in the /dags 
SQL related to each task are stored /dags/sql folder 

Tasks 
  
1. delta_load_lock_ts: Set start window for the current job. 

2. load_stg_dim_content: Prepare stage table related to content dim 

3. load_dim_content: Load content dim from stage table 

4. load_source_stg_session_pageview: Access pageview table based on the partition and retrieve required new rows based on the start         window set via first task, goal was to only process records created in source since last execution, similar to CDC. Create a source     dependent stage table that will be utilized for tasks 5, 6, 8. 

5. load_dim_page: Prepare stage table related to page dim 

6. load_dim_stage_sessions: Prepare stage table related to page dim, load data to target dim_page 

7. load_dim_sessions: Load session dim from stage table 

8. load_stg_page_view_by_session: Prepare stage table related to page fact page_view_by_session 

9. load_page_view_by_session: Load page_view_by_session from stage table 

10. delta_load_unlock_ts: Update current job end date, close window. 

Based on the windowing process defined in task 1 and 10, we have fault tolerance for today’s executions. If for some reason, and task between 1 and 10 fails, during the next the dag execution the start window will be set to the last completed job, hence maintaining data freshness. 


code-assignment: SimpleAPI

For this exercise, once data has been extratced from the api and stored in the required data repositor, the next tasks within the DAG would be to update the existing target table using state abbreviation as the key and governor last and first names as attributes. Considering there are only 50 states, this should be a fairly straight forward.
