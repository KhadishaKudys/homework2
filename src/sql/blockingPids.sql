SELECT
    a.pid AS blocked_pid,
    b.pid AS blocking_pid,
    b.query AS blocking_query,
    now() - a.query_start AS blocked_time
FROM pg_stat_activity a
         JOIN pg_stat_activity b
              ON b.pid = ANY(pg_blocking_pids(a.pid))