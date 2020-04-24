# code-assignment
code-assignment



Assumptions:

I am designing the underlying physical data model(facts, dims) based on the two tables(pageviews, and content). Product owner will then design the datasets/metadata layer in Tableau, Looker etc, utilizing these fact and dim tables, addressing each of their usecases.
Initial full load, followed by incremental loads.

Questions:
Thank you Alex for answering these!!!!!

1. How often is data loaded into pageview and content tables? Realtime/batch?
--Let’s assume the data is loaded in near real-time, a batch every 5 mins.
2. How often do the fact and dim tables need to be updated? Daily, Hourly, etc.
--For the data freshness, let’s set 1 hr as our target
3. Are these source tables partitioned, to avoid full table scans while accessing them.  
4. Content table has a version column. Is a new record inserted into the content table with an incremental version number? i.e Can rows duplicate for each contentId, but the grain is at the contentId + version level? Or existing record updated with a new version number?
For this slowly chaining dim, is there an SCD requirement (1,2,3)?
Or for this exercise should I assume there is always only one record per contentID in the content table.
5. For the average user session duration trends requirement, I see sessionId on the pageviews table, but no additional metadata related to sessionId.
6. I am a bit confused between pageUrl and referrerUrl. Is referreURL the parent page from when the user navigated to the current page, and so forth?
