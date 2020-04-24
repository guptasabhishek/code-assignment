/* Populate page dim, read from stg_session_pageview table identify new records and insert into dim_page stage*/
/*I am only inserting new records, assuming the URl does not update. I did not foresee the need to perform an merge/insert*/
/*Additionally consideration, based on exact needs to suit performace requirements. 
This entire process could be broken down further, e.g materialize results to intermediate/temp tables, and read from those tables
as compared to subqueries. */

INSERT INTO dim_page (/*id,*/ url)

SELECT
#surrogate key for the new records inserted. Depending on the database could be ROWID, or next value based on a sequence.
--id
,url
FROM
(
SELECT
ISNULL(dim_page.id, 0) AS id
,srs_stg_dim_page.url
FROM
(
SELECT DISTINCT
referrerUrl AS url
FROM 
stg_session_pageview
WHERE 
1=1

UNION ALL

SELECT DISTINCT
pageUrl AS url
FROM 
stg_session_pageview
WHERE 
1=1
) srs_stg_dim_page
LEFT JOIN dim_page ON 
srs_stg_dim_page.url = dim_page.url
)
WHERE 
id = 0


