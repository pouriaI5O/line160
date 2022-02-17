{{
    config(
        materialized='incremental'
    )
}}

with cte as(
---source table for id tabels (line160)---
---Extract timestamps info(date,hour,minute and second) and rename other columns---
select   timestamp as Timestamps,
          EXTRACT(HOUR FROM timestamp) AS Hour,
          EXTRACT(MINUTE FROM timestamp) AS Minute,
          EXTRACT(SECOND FROM timestamp) AS Second, 
          DATE(timestamp) as Date,
          personline160 as Count_Person,
          line160wip as Count_Wip,
          line160wt as Count_Wt                      
FROM {{ source('public','pridemobility_tracking_160_new') }}
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where timestamp > (select max(timestamp) from {{ this }})

{% endif %}),
cte1 as(select ceiling(avg(Count_Wt)) as Count_Wt,
       ceiling(avg(Count_Wip)) as Count_Wip,
       ceiling(avg(Count_Person)) as Count_Person,
       Date,
       Hour,
       Minute,
       Second 
from cte
group by Date,Hour,Minute,Second),
cte2 as (select Date,
       Hour,
       Minute ,
       round(avg(Count_Person)) as Count_Person ,
       round(avg(Count_Wip))as Count_Wip,
       round(avg(Count_Wt))as Count_Wt
from cte1 
group by Date,Hour,Minute
order by Date),
cte3 as (select hour,
       minute,
       date,
       Count_Wt,
       Count_Person,
       Count_Wip,
       160 as station,
       cast(CONCAT(date, ' '+CAST(hour AS VARCHAR(2))+':'+CAST(minute AS VARCHAR(2))) as datetime)as Timestamps,
       cast(Timestamps AS time) as Time,
       convert_timezone('America/New_York','UTC', Timestamps)  AS  utc_timestamp ,
CASE WHEN Count_Person > 0 and count_wip>0 THEN 'Productive'
     WHEN Count_Person = 0 and count_wip>0 THEN 'Unproductive'
     WHEN Count_Person > 0 and count_wip=0 THEN 'Downtime'
     ELSE 'Idle' END AS Status,
CASE WHEN Count_Person > 0 and count_wip>0 THEN 'Productive'
     ELSE 'Wast' END AS Ole_Status    
from cte2 
where (Time >'06:00:00' and Time <'14:30:00')),
cte4 as(select *,CASE WHEN Time>'08:10:00' and Time <'08:25:00'THEN 0
     WHEN Time >'10:45:00' and Time <'11:25:00'THEN 0
     WHEN Time >'12:50:00' and Time <'13:05:00'THEN 0
     ELSE 1 END AS Break_Filter
from cte3
where Break_Filter>0),
cte5 as (select convert_timezone('America/New_York','America/New_York',timestamps) as local_timestamp,
       utc_timestamp,
       hour,
       minute,
       date,
       count_person,
       count_wip,
       count_wt,
       station,
       status,
       ole_status as status_ole 
FROM cte4)
select * from cte5





