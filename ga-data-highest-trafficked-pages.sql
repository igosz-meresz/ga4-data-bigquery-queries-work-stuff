WITH pages AS (
SELECT 
    user_pseudo_id,event_name,
    MAX(CASE WHEN key = "page_title" THEN value.string_value ELSE NULL END) AS page,
    MAX(CASE WHEN event_name = 'page_view' and key = 'page_title' THEN value.string_value ELSE NULL END) AS pageTitle,
    MAX (CASE WHEN params.key = "ga_session_id" THEN params.value.int_value ELSE 0 END) AS sessionId,
    CASE WHEN event_name = "first_visit" then 1 else 0 END AS newUsers,
    MAX((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged')) as sessionEngaged,
    MAX(CASE WHEN key =  "engagement_time_msec" then value.int_value else 0 END) AS engagementTimeMsec,
    MAX(CASE WHEN event_name = "scroll" AND params.key = "percent_scrolled" THEN params.value.int_value ELSE 0 END) AS percentageScroll,
    -- Change event_name to include any/all conversion event(s) to show the count
    COUNTIF(event_name = 'select_content' AND key = "page_title") AS conversions,
    SUM(ecommerce.purchase_revenue) AS totalRevenue
FROM
  --- Update the below dataset to match your GA4 dataset and project
  `ga4-data-344909.analytics_**********.events_*`, UNNEST (event_params) AS params
--WHERE _table_suffix BETWEEN '20210301' AND '20210331'
GROUP BY 
user_pseudo_id,
event_name),
-- Extract engagement time,pageCount and eventCount data
pageTop AS (
SELECT
  user_pseudo_id, 
  event_date, 
  event_timestamp, 
  event_name, 
  MAX(CASE WHEN event_name = 'page_view' AND params.key = "page_title" THEN params.value.string_value END) AS pageCount,
  MAX(CASE WHEN params.key = "page_title" THEN params.value.string_value ELSE NULL END) AS page,
  MAX(CASE WHEN params.key = "engagement_time_msec" THEN params.value.int_value/1000 ELSE 0 END) AS engagementTimeMsec
FROM
  `ga4-data-344909.analytics_**********.events_*`, unnest(event_params) as params

--WHERE _table_suffix BETWEEN '20220301' AND '20220331'
GROUP BY user_pseudo_id, event_date, event_timestamp, event_name
),
--Summarize data for average engagement time, Views, Users, viewsPerUser and eventCount
pageTopSummary AS (
SELECT 
  page, 
  ROUND (SAFE_DIVIDE(SUM(engagementTimeMsec),COUNT(DISTINCT user_pseudo_id)),2) AS avgEngagementTime,
  COUNT (pageCount) AS Views,
  COUNT (DISTINCT user_pseudo_id) AS Users,
  ROUND(COUNT (pageCount)/COUNT (DISTINCT user_pseudo_id),2) AS viewsPerUser

FROM 
  pageTop
GROUP BY 
  page)
-- MAIN QUERY
SELECT 
    sub.page,
    Views,
    Users,
    newUser,
    viewsPerUser,
    avgEngagementTime,
    uniqueUserscrolls,
    conversions,
    totalRevenue
FROM (
SELECT 
    page,
    SUM (newUsers) as newUser,
    COUNT(CASE WHEN percentageScroll = 90 THEN user_pseudo_id END) AS uniqueUserscrolls,
    SUM(conversions) AS conversions,
    CONCAT('PLN', IFNULL(SUM(totalRevenue),0)) AS totalRevenue
FROM 
    pages
WHERE page IS NOT NULL
GROUP BY 
    page)
-- Sub query to joining summary reports together 
sub
LEFT JOIN  pageTopSummary
ON 
  pageTopSummary.page = sub.page
ORDER BY 
    Users  DESC