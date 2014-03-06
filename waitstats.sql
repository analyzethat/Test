-- Longest waiting items:

SELECT wait_type, signal_wait_time_ms, wait_time_ms
FROM sys.dm_os_wait_stats
where wait_time_ms >0
and wait_type not in 
('CLR_SEMAPHORE', 'LAZYWRITER_SLEEP','RESOURCE_QUEUE', 'SLEEP_TASK', 'SLEEP_SYSTEMTASK', 
'SQLTRACE_BUFFER_FLUSH', 'WAITFOR', 'LOGMGR_QUEUE', 'CHECKPOINT_QUEUE', 'REQUEST_FOR_DEADLOCK_SEARCH',
'XE_TIMER_EVENT', 'BROKER_TO_FLUSH', 'BROKER_TASK_STOP', 'CLR_MANUAL_EVENT', 'CLR_AUTO_EVENT', 
'DISPATCHER_QUEUE_SEMAPHORE', 'FT_IFTS_SCHEDULER_IDLE_WAIT', 'XE_DISPATCHER_WAIT', 'XE_DISPATCHER_JOIN',
'SQLTRACE_INCREMENTAL_FLUSH_SLEEP') -- geen system related waits
order by signal_wait_time_ms Desc
-- PREEMPTIVE OS: Wacht op de scheduler van het Operating System
-- PAGEIOLATCH_EX: Lange wachttijden tijdens het laden van de disk
-- PAGEIOLATCH_DT/KP/SH/UP: DT: destroy KP: keep SHL shared Up:?
-- Redenen voor hoge PAGEIOLATCH: trage disks, memory pressure, file placing (LDF, MDF en tempdb op verschillende schijven), indexen
-- IO_COMPLETION hoog: langzame disk, ligt de oorzaak in het SAN? probeer dan een hogere HBA Queue Depth
-- ASYNC_IO_COMPLETION
-- WRITELOG: SQL Server heeft tijd nodig om de log te schrijven


-- Perfmon counters: 
-- - Memory Manager\Memory Grants Pending (hoger dan 0-2)
-- - Memory Manager\Memory Grants Outstanding (consistent hoog)
-- - Buffer Manager\Buffer Hit Cache Ratio (hoger = beter, boven de 90% graag)
-- - Buffer Manager\Page Life Expectancy (consistent laag)
-- - Memory: Pages\sec
-- - Average Disk sec\Read (niet hoger dan 4-8 milliseconde)
-- - Average Disk sec\Write (niet hoger dan 4-8 milliseconde)
-- - Average Disk Read\Write Queue Length



-- How much wait time on signal vs resource
-- CPU_waits = hoe lang moet er gewacht worden op een signal
-- CPU-resource = hoeveel tijd is besteed aan query running / suspended query's

SELECT
CAST(100.0 * sum(signal_wait_time_ms) / sum(wait_time_ms) as numeric(20,2)) as [%CPU_WAITS],
CAST(100.0 * sum(wait_time_ms - signal_wait_time_ms) / sum(wait_time_ms) as numeric(20,2)) as [%CPU-RESOURCE]
FROM sys.dm_os_wait_stats
where wait_time_ms >0
and wait_type not in 
('CLR_SEMAPHORE', 'LAZYWRITER_SLEEP','RESOURCE_QUEUE', 'SLEEP_TASK', 'SLEEP_SYSTEMTASK', 
'SQLTRACE_BUFFER_FLUSH', 'WAITFOR', 'LOGMGR_QUEUE', 'CHECKPOINT_QUEUE', 'REQUEST_FOR_DEADLOCK_SEARCH',
'XE_TIMER_EVENT', 'BROKER_TO_FLUSH', 'BROKER_TASK_STOP', 'CLR_MANUAL_EVENT', 'CLR_AUTO_EVENT', 
'DISPATCHER_QUEUE_SEMAPHORE', 'FT_IFTS_SCHEDULER_IDLE_WAIT', 'XE_DISPATCHER_WAIT', 'XE_DISPATCHER_JOIN',
'SQLTRACE_INCREMENTAL_FLUSH_SLEEP') -- geen system related waits

--All currently running wait stats (blocking_session_id is gevuld als een session wacht op een lock)
select *
from sys.dm_os_waiting_tasks

-- Wait types met de query erbij (vooral voor queries die nu draaien of waarvan het query plan nog in de cache zit
SELECT
dm_ws.wait_type,
dm_es.status,
dm_t.[text],
dm_ws.session_id,
dm_es.cpu_time,
dm_es.memory_usage,
dm_es.logical_reads,
dm_es.total_elapsed_time,
dm_es.program_name,
DB_NAME(dm_r.database_id) as DatabaseName,
dm_ws.blocking_session_id,
dm_r.wait_resource,
dm_es.login_name,
dm_r.command,
dm_r.last_wait_type
from sys.dm_os_waiting_tasks dm_ws
join sys.dm_exec_requests dm_r on dm_ws.session_id = dm_r.session_id
join sys.dm_exec_sessions dm_es on dm_es.session_id = dm_r.session_id
cross apply sys.dm_exec_sql_text (dm_r.sql_handle) dm_t
where dm_es.is_user_process = 1

-- Parallelle executie (CXPacket wait ontstaat als 2 of meer threads tegelijk executeren, maar niet tegelijk klaar zijn)
-- De cost treshold bepaalt wanneer SQL Server parallel gaat werken en max degree of parallelism bepaalt de hoeveelheid threads

-- Misschien moet je advanced options aanzetten om dit te doen: (voor veiligheid uitgecommentarieerd)
-- EXEC sys.sp_configure 'show advanced options', N'1'

-- Instellen cost treshold / max degree (voor veiligheid uitgecommentarieerd) --> voorbeeldinstellingen: treshold bij 25 sec winst en 2 cpus max degree
-- EXEC sys.sp_configure N'cost treshold for parallelism', N'25'
-- GO
-- EXEC sys.sp_configure N'max degree of parallelism', N'2'
-- GO
-- RECONFIGURE WITH OVERRIDE
-- GO

-- Runnable tasks tijdens execution vinden, >10 runnable tasks count laat druk op de CPU zien
select scheduler_id, current_tasks_count, runnable_tasks_count, work_queue_count, pending_disk_io_count
from sys.dm_os_schedulers
where scheduler_id < 255 

-- Queries die veel resources gebruiken
select SUBSTRING(qt.text, (qs.statement_start_offset/2)+1, ((case qs.statement_end_offset
when -1 then DATALENGTH(qt.text)
else qs.statement_end_offset end - qs.statement_start_offset)/2)+1),
qs.execution_count,
qs.total_logical_reads,
qs.last_logical_reads,
qs.total_logical_writes,
qs.last_logical_writes,
qs.total_worker_time,
qs.last_worker_time,
qs.total_elapsed_time/1000000 as total_elapsed_time_in_S,
qs.last_elapsed_time/1000000 as last_elapsed_time_in_S,
qs.last_execution_time,
qp.query_plan
from sys.dm_exec_query_stats qs
cross apply sys.dm_exec_sql_text(qs.sql_handle) qt
cross apply sys.dm_exec_query_plan(qs.plan_handle) qp
order by qs.total_worker_time desc --(cpu time)

