
create or replace table `nbcu-ds-sandbox-a-001.Shunchao_Sandbox_Final.Top_Feeder_Metadata_Original` as 

with rank_set as (select 
Adobe_Date,
Feeder_Video as Auto_Binge_Source_Titles,
Unique_Auto_Binge_Accounts,
Unique_Click_Next_Accounts,
Total_Unique_Accounts,
dense_rank() over (partition by Adobe_Date order by Total_Unique_Accounts desc) as Daily_Ranks
from
(select 
Adobe_Date,
Feeder_Video,
count(distinct case when Video_Start_Type = "Auto-Play" then Adobe_Tracking_ID else null end) as Unique_Auto_Binge_Accounts,
count(distinct case when Video_Start_Type = "Clicked-Up-Next" then Adobe_Tracking_ID else null end) as Unique_Click_Next_Accounts,
count(distinct case when Video_Start_Type in ("Clicked-Up-Next","Auto-Play") then Adobe_Tracking_ID else null end) as Total_Unique_Accounts,
from `nbcu-ds-sandbox-a-001.Shunchao_Sandbox_Final.Auto_Binge_Metadata_Prod`
where 1=1
and Adobe_Date = current_date("America/New_York")-1
and Feeder_Video != "" and Feeder_Video is not null
and Feeder_Video != "view-all" -- remove epsiode-to-epsiode cases and "View-All
and Feeder_Video not in (SELECT 
                         regexp_replace(lower(content_channel), r"[:,.&'!]", '')
                         FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO` 
                         WHERE 1=1
                         and adobe_date = current_date("America/New_York")-1
                         and content_channel != "N/A"
                         group by 1)  -- remove linear channels from the result
group by 1,2) a)

select 
Adobe_Date,
Daily_Ranks,
Auto_Binge_Source_Titles,
Unique_Auto_Binge_Accounts,
Unique_Click_Next_Accounts,
Total_Unique_Accounts
from rank_set
where Daily_Ranks <= 50
union all
select *
from `nbcu-ds-sandbox-a-001.Shunchao_Sandbox_Final.Top_Feeder_Metadata_Original`
order by 1,6 desc


