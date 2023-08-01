--EPG monthly Peak Throughput without spike for linkgroup
with  epg_linkgroup_octets as (
        select c.exchange_code as EPG, a.metrics_date, a.logical_name, a.epochtime, a.policing_policy_name1 as POI, a.dt
            , description
            , try_cast((a.out_octets - lag(a.out_octets, 1, a.out_octets) OVER (partition by a.logical_name, a.description ORDER BY a.logical_name, a.epochtime asc)) as double) as OutOctets
            , try_cast((a.in_octets - lag(a.in_octets, 1, a.in_octets) OVER (partition by a.logical_name, a.description ORDER BY a.logical_name, a.epochtime asc)) as double) as InOctets          
            , try_cast((a.epochtime - lag(a.epochtime, 1, a.epochtime) OVER (partition by a.logical_name, a.description ORDER BY a.logical_name ,a.epochtime asc)) as double) as Interval_seconds
        from (select 
        	 (case
	                    when policing_policy_name = '2WIN' then '2RCH'
	                    when policing_policy_name = '4CAB' then '4NIN'
	                    when policing_policy_name = '4PRM' then '4TOB'
	                else policing_policy_name
	                end) as policing_policy_name1
		, * from hive.thor_fw.v_union_ossrc_enm_epg_policy_linkgroup where dt > 20200101) a
        inner join
            hive.thor_fw.v_thor_epg_lwr_mapping c
        on (a.logical_name = c.epg_logical_name)
        --where a.description like '%lg-RAN%' and c.exchange_code like '2WAG'
        order by c.exchange_code, a.logical_name, a.description, a.metrics_date),
    epg_linkgroup_throughput as (
        select 
        	EPG
        	, metrics_date
        	, logical_name
        	, epochtime
        	, description
        	, POI
            , case when Interval_seconds = 0  then 0 else round((OutOctets * 8 / Interval_seconds / 1000000), 0) end as EgressLinkgroup
            , case when Interval_seconds = 0 or date(metrics_date) = date(date_parse('2/18/2021 6:05:04 PM','%m/%d/%Y %h:%i:%s %p')) then 0 else round((InOctets * 8 / Interval_seconds / 1000000), 0) end as IngressLinkgroup      
        from epg_linkgroup_octets ),
    epg_linkgroup_throughput_nospike as (
        select a.*
            , case when EgressLinkgroup is NULL
                then 0
                else lag(EgressLinkgroup, 2, NULL) OVER (partition by EPG, description ORDER BY EgressLinkgroup)
                end as ThirdPeakEgress
            , case when lag(EgressLinkgroup, 1, NULL) OVER (partition by logical_name, description ORDER BY epochtime) < EgressLinkgroup * 0.9
                then lag(EgressLinkgroup, 1, NULL) OVER (partition by logical_name, description ORDER BY epochtime)
                else EgressLinkgroup
              end as NoSpikeEgress
            , case when IngressLinkgroup is NULL
                then 0
                else lag(IngressLinkgroup, 2, NULL) OVER (partition by EPG, description ORDER BY IngressLinkgroup)
                end as ThirdPeakIngress
            , case when lag(IngressLinkgroup, 1, NULL) OVER (partition by logical_name, description ORDER BY epochtime) < IngressLinkgroup * 0.9
                then lag(IngressLinkgroup, 1, NULL) OVER (partition by logical_name, description ORDER BY epochtime)
                else IngressLinkgroup
              end as NoSpikeIngress
        from epg_linkgroup_throughput a),
    epg_linkgroup_throughput_peak as (
        select EPG , date(metrics_date) as Date 
            , case when description like 'lg-EAS%' then 'lg-EAS' else description end as description
            , POI
            , case when max(NoSpikeEgress)  > max(NoSpikeIngress) then max(NoSpikeEgress) else max(NoSpikeIngress) end as DownlinkThroughput
             from epg_linkgroup_throughput_nospike
        group by 1, 2, 3, 4
        order by 2, 1)
select *
from epg_linkgroup_throughput_peak

/*OLD SCRIPT- lg-RAN YTD with LAG function*/							 
select
		Logical
		, EPG
		, Date
		--, Hour
		, case 
			when coalesce(DownlinkThroughput,0) between 0 and 130000 then cast(DownlinkThroughput as double) 
			end as DownlinkThroughput
		--, cast (UplinkThroughput as double) UplinkThroughput
	from
	(	
	select 	
		Logical
		, EPG
		, Date
		--,  Hour
		, case when max (octets_in * 8/ period_duration / 1000000) > max (octets_out * 8/ period_duration / 1000000) then max (octets_in * 8/ period_duration/ 1000000) else max (octets_out * 8/ period_duration / 1000000) end as DownlinkThroughput			
	from 
		(
			select 
				a.logical_name as Logical
				, c.exchange_code as EPG
				, date(at_timezone(a.metrics_date,'Australia/Sydney')) as Date
				--, hour(at_timezone(a.metrics_date,'Australia/Sydney')) as Hour
				, /**//*a.logical_name, */ a.description
				, a.policing_policy_name as POI
				, try_cast((a.out_octets - lag(a.out_octets, 1, a.out_octets) OVER (partition by a.logical_name, a.description ORDER BY a.logical_name, a.metrics_date asc)) as double ) AS octets_out
				, try_cast((a.in_octets - lag(a.in_octets, 1, a.in_octets) OVER (partition by a.logical_name, a.description ORDER BY a.logical_name, a.metrics_date asc)) as double) AS octets_in
				, try_cast(date_diff('second',lag(a.metrics_date, 1, a.metrics_date) OVER (partition by a.logical_name, a.description ORDER BY a.logical_name ,a.metrics_date asc), a.metrics_date) as double) period_duration
		from 
			(SELECT * FROM hive.thor_fw.v_union_ossrc_enm_epg_policy_linkgroup  WHERE dt >= 20210101 and out_octets >0) a
                     inner join hive.thor_fw.v_thor_epg_lwr_mapping c
                     on (a.logical_name = c.epg_logical_name)
                     where a.logical_name like '%WCPGW%'
                            --and c.exchange_code like '3BEN'
                             /*and a.policing_policy_name like '3BEN'*/
                             and  a.description like '%lg-RAN%')
	group by Logical, EPG,Date, POI
	)
	group by 1, 2, 3, 4 
/*lg-RAN YTD with LAG function*/	



/*NOT USED - LGRAN YTD using LWR source*/
select 
			LWR
		--, Year(metrics_date) as Year
			, DATE(metrics_date) as Date
			, max(DownlinkThroughput) as DownlinkThroughput
			--, max(UplinkThroughput) as UplinkThroughput
		from
			(
				select 
					LWR
					, logical_name
					, metrics_date
					, sum(DownlinkThroughput) as DownlinkThroughput
					, sum(UplinkThroughput) as UplinkThroughput
				from
				(
					select a.logical_name, c.exchange_code as LWR, at_timezone(a.metrics_date,'Australia/Sydney') metrics_date, a.dt, a.hour, a.if_name, a.if_alias						
						, case 
							when a.if_name like '%TenGigE%' and (a.if_hc_in_octets )  / 1000000 * 8 / 15/ 60 > 10000 then 0
							when a.if_name like '%HundredGigE%' and (a.if_hc_in_octets )  / 1000000 * 8 / 15/ 60 > 100000 then 0							
								else (a.if_hc_in_octets )  / 1000000 * 8 / 15/ 60									
									end as DownlinkThroughput
						, case 
							when a.if_name like '%TenGigE%' and (a.if_hc_out_octets )  / 1000000 * 8 / 15/ 60 > 10000 then 0
							when a.if_name like '%HundredGigE%' and (a.if_hc_out_octets )  / 1000000 * 8 / 15/ 60 > 100000 then 0							
								else (a.if_hc_out_octets )  / 1000000 * 8 / 15/ 60									
									end as UplinkThroughput						
					from (select * from thor_fw.v_union_lwr_mfile_ifhc where dt >= 20200801/* and dt < 20210201*/ ) a
					inner join
						(select * from hive.thor_fw.v_thor_epg_lwr_mapping) c
					on (a.logical_name = c.lwr_logical_name)
					/*and a.if_alias <> ''*/
					--where (a.metrics_date between last_day_of_month(current_date - interval '2' month) + interval '1' day AND last_day_of_month(current_date - interval '1' month )+ interval '1' day)
					where a.dt >= 20200801 /*and a.dt < 20210201*/
					and (a.if_name like '%TenGigE%'  or a.if_name like '%HundredGigE%' /**/)
					--and (c.exchange_code like '%2CFS%')-- or c.exchange_code like '%3MEB%' or c.exchange_code like '%3BEN%')*/
					and a.if_alias like '%To-EPG%'
					and a.logical_name like '%%'/**/
					)
				group by LWR, logical_name, metrics_date
			)
		group by 1,2
		order by 1, 2
/*LGRAN YTD using LWR source*/
								 					 