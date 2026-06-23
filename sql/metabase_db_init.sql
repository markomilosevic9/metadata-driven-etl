-- metabase metadata database init
-- automatically executed on postgres first startup
-- kept separate from warehouse schema initialization so analytics ddl and bi app bootstrap remain decoupled

SELECT '
CREATE DATABASE metabase
    WITH
    OWNER = analytics
    ENCODING = ''UTF8''
'
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_database
    WHERE datname = 'metabase'
)\gexec