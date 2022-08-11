DROP MATERIALIZED VIEW last_revised_files;
CREATE MATERIALIZED VIEW last_revised_files AS
SELECT
    file.id AS file_id,
    left(file.name, 40) AS tr_file_name,
    revision.id AS rev_id,
    -- date_trunc('seconds', revision.updated) AS rev_updated
    revision.updated AS rev_updated
FROM
    file
    LEFT JOIN revision ON file.id = revision.file_id
ORDER BY
    revision.updated ASC
LIMIT
    1000 WITH DATA;

DROP MATERIALIZED VIEW revisions_by_file;
CREATE MATERIALIZED VIEW revisions_by_file AS
SELECT
    file.name AS file_name,
    file."mimeType" AS file_type,
    max(revision.updated) AS last_update,
    count(revision.id) AS nrevision,
    file.id AS file_id,
    file.path as file_path
FROM
    file
    LEFT JOIN revision ON file.id = revision.file_id
GROUP BY file.id
ORDER BY
    nrevision DESC
LIMIT
    1000 WITH DATA;

DROP MATERIALIZED VIEW vw_file_sessions;
CREATE MATERIALIZED VIEW vw_file_sessions AS
SELECT
    file_session.id AS sid,
    count(file.id) AS nfile,
    file_session.nused AS nused,
    date_trunc('seconds', file_session.updated) AS last_updated,
    -- string_agg(left(file.name, ROUND(160/nfile)), ', ') AS file_name_agg
    string_agg(left(file.name, 30), ', ') AS file_name_agg
FROM
    file_session_association AS fsa
    LEFT JOIN file ON file.id = fsa.file_id
    LEFT JOIN file_session ON file_session.id = fsa.file_session_id
GROUP BY file_session.id
ORDER BY
    -- nused DESC, last_updated DESC
    last_updated DESC
LIMIT
    1000 WITH DATA;

