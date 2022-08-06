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
    left(file.name, 40) AS tr_file_name,
    file."mimeType" AS file_type,
    max(revision.updated) AS last_update,
    count(revision.id) AS nrevision
FROM
    file
    LEFT JOIN revision ON file.id = revision.file_id
GROUP BY file.id
ORDER BY
    nrevision DESC
LIMIT
    1000 WITH DATA;

