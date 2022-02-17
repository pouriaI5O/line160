{{
    config(
        materialized='incremental'
    )
}}
with cte as (
---source table for Status tabels(line160)---
---Extract timestamps info(date,hour,minute asn second) and rename other columns
select idline160wip as Id_Wip,
       idline160wt as Id_Wt,
       EXTRACT(HOUR FROM timestamp) AS Hour,
       EXTRACT(MINUTE FROM timestamp) AS Minute,
       EXTRACT(SECOND FROM timestamp) AS Second, 
       DATE(timestamp) as Date          
FROM {{ source('public','pridemobility_tracking_160_new') }}
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where timestamp > (select max(timestamp) from {{ this }})

{% endif %}),
cte1 as(select*,
        cast(CONCAT(Date, ' '+CAST(hour AS VARCHAR(2))+':'+CAST(minute AS VARCHAR(2))) as datetime)as Timestamps,
        cast(Timestamps AS time) as Time 
from cte
where  (Time >'06:00:00' and Time <'14:30:00')
),
cte2 as (select *,
                      CASE WHEN Time>'08:10:00' and Time <'08:25:00'THEN 0
                           WHEN Time >'10:45:00' and Time <'11:25:00'THEN 0
                           WHEN Time >'12:50:00' and Time <'13:05:00'THEN 0
                           ELSE 1 END AS Break_Filter
              from cte1 where Break_Filter>0),
 ---select all columns     
cte3 as (select 1 as t_second,
       Id_Wip,
       Date,
       Hour,
       minute,
       second 
from cte2
where Id_Wip is not null 
group by Id_Wip,Date,Hour,minute,second
order by Date),
cte4 as (select sum(t_second) as duration,
       hour,
       Date,
       Id_Wip
from cte3
group by date,hour,Id_Wip),
cte5 as(select hour,
                   Date,
                   Id_Wip as chair_id,
                   Duration,
                   cast(CONCAT(Date, ' '+CAST(hour AS VARCHAR(2))+':'+00) as datetime)as Timestamps,
                   cast(Timestamps AS time) as Time,
                   convert_timezone('America/New_York','UTC', Timestamps)  AS  utc_timestamp  
            from cte4 order by date,hour,Id_Wip)
select *,
       convert_timezone('UTC','America/New_York',utc_timestamp) as local_timestamp 
from cte5