CREATE TABLE fabmq_meta (
    key STRING PRIMARY KEY,
    value STRING NOT NULL
);

CREATE TABLE mq (
    bucket INT2 NOT NULL,
    topic STRING NOT NULL,
    seq_id INT8 NOT NULL,
    job_id INT8 NOT NULL AS ((bucket << 55:::INT8) | seq_id) VIRTUAL,
    payload STRING NOT NULL,
    created_at TIMESTAMPTZ NULL DEFAULT now(),
    CONSTRAINT pk_mq PRIMARY KEY (bucket ASC, topic ASC, seq_id ASC),
    CONSTRAINT check_bucket CHECK (bucket BETWEEN 0 AND 255)
);

ALTER TABLE mq
  SET (ttl = 'on', ttl_expiration_expression = e'
        CASE
            WHEN seq_id = 0 THEN NULL
            ELSE created_at + interval \'14 days\'
        END',
        ttl_job_cron = '@hourly');

-- splitting the table into 256 ranges
alter table mq split at select g from generate_series(0,255) as g;
alter table mq scatter;

-- high watermark
CREATE TABLE hwm (
    bucket INT2 NOT NULL,
    topic STRING NOT NULL,
    consumer_group STRING NOT NULL,
    last_seq_id INT8 NOT NULL,
    last_job_id INT8 NOT NULL AS ((bucket << 55:::INT8) | last_seq_id) VIRTUAL,
    completed_at TIMESTAMPTZ NULL DEFAULT now():::TIMESTAMPTZ,
    CONSTRAINT pk_hwm PRIMARY KEY (bucket, topic, consumer_group, last_seq_id),
    CONSTRAINT check_bucket CHECK (bucket BETWEEN 0 AND 255)
);

ALTER TABLE hwm
    SET (ttl = 'on', ttl_expiration_expression = e'
        CASE
            WHEN last_seq_id = 0 THEN NULL
            ELSE completed_at + interval \'14 days\'
        END',
        ttl_job_cron = '@hourly');

-- splitting hwm table into 256 ranges
alter table hwm split at select g from generate_series(0,255) as g;
alter table hwm scatter;



-- Function to create a new topic
CREATE OR REPLACE FUNCTION create_topic(p_topic STRING)
RETURNS STRING
LANGUAGE PLpgSQL
VOLATILE
AS $$
BEGIN
    IF p_topic IS NULL OR length(trim(p_topic)) = 0 THEN
        RAISE EXCEPTION 'topic cannot be null or empty';
    END IF;

    INSERT INTO mq (
        bucket,
        topic,
        seq_id,
        payload,
        created_at
    )
    SELECT
        i::INT2,
        p_topic,
        0,
        '<- ' || p_topic || ' ->',
        NULL
    FROM generate_series(0, 255) AS g(i)
    ON CONFLICT (bucket, topic, seq_id) DO NOTHING;

    RETURN p_topic;
END;
$$;

CREATE OR REPLACE FUNCTION delete_topic(p_topic STRING)
RETURNS STRING
LANGUAGE PLpgSQL
VOLATILE
AS $$
BEGIN
    IF p_topic IS NULL OR length(trim(p_topic)) = 0 THEN
        RAISE EXCEPTION 'topic cannot be null or empty';
    END IF;

    DELETE FROM mq
    WHERE (bucket, topic, seq_id) = ANY (
    SELECT
        i::INT2,
        p_topic,
        0
    FROM generate_series(0, 255) AS g(i));

    RETURN p_topic;
END;
$$;

CREATE OR REPLACE FUNCTION list_topics()
RETURNS TABLE (
    topic STRING
)
LANGUAGE SQL
STABLE
AS $$
    SELECT DISTINCT topic
    FROM mq
    ORDER BY topic;
$$;


-- Function to add new jobs
CREATE OR REPLACE FUNCTION enqueue_job(p_topic STRING, p_payload STRING)
RETURNS INT8
LANGUAGE PLpgSQL
VOLATILE
AS $$
DECLARE
    v_bucket INT2;
    v_job_id INT8;
BEGIN
    IF p_payload IS NULL THEN
        RAISE EXCEPTION 'payload cannot be null';
    END IF;

    IF p_topic IS NULL OR length(trim(p_topic)) = 0 THEN
        RAISE EXCEPTION 'topic cannot be null or an empty string';
    END IF;

    v_bucket := (crc32ieee(p_payload) % 256)::INT2;

    WITH acquire_lock AS (
        SELECT bucket, topic
        FROM mq
        WHERE bucket = v_bucket
          AND topic = p_topic
          AND seq_id = 0
        FOR UPDATE
    ),
    next_position AS (
        SELECT mq.seq_id + 1 AS next_seq_id
        FROM mq
        WHERE (bucket, topic) = (v_bucket, p_topic)
        ORDER BY seq_id DESC
        LIMIT 1
    )
    INSERT INTO mq (
        bucket,
        topic,
        seq_id,
        payload
    )
    SELECT
        v_bucket,
        p_topic,
        next_seq_id,
        p_payload
    FROM next_position, acquire_lock
    RETURNING job_id
    INTO v_job_id;

    IF v_job_id IS NULL THEN
        RAISE EXCEPTION
            'missing lock sentinel for bucket %',
            v_bucket;
    END IF;

    RETURN v_job_id;
END;
$$;

CREATE OR REPLACE FUNCTION enqueue_jobs(
    p_topic STRING,
    p_payloads STRING[]
)
RETURNS INT8
LANGUAGE PLpgSQL
VOLATILE
AS $$
DECLARE
    v_bucket INT2;
    v_job_id INT8;
BEGIN
    IF p_topic IS NULL OR length(trim(p_topic)) = 0 THEN
        RAISE EXCEPTION 'topic cannot be null or empty';
    END IF;

    IF p_payloads IS NULL OR array_length(p_payloads, 1) IS NULL THEN
        RAISE EXCEPTION 'payloads cannot be null or empty';
    END IF;

    -- Entire batch is routed based on the first payload.
    v_bucket := (crc32ieee(p_payloads[1]) % 256)::INT2;

    WITH acquire_lock AS (
        SELECT bucket
        FROM mq
        WHERE topic = p_topic
          AND bucket = v_bucket
          AND seq_id = 0
        FOR UPDATE
    ),
    current_tail AS (
        SELECT seq_id AS last_seq_id
        FROM mq
        WHERE (bucket, topic) = (v_bucket, p_topic)
        ORDER BY seq_id DESC
        LIMIT 1
    ),
    batch AS (
        SELECT
            payload,
            ordinality::INT8 AS offset
        FROM unnest(p_payloads)
             WITH ORDINALITY AS u(payload, ordinality)
    )
    INSERT INTO mq (
        bucket,
        topic,
        seq_id,
        payload
    )
    SELECT
        v_bucket,
        p_topic,
        current_tail.last_seq_id + batch.offset,
        batch.payload
    FROM current_tail
    CROSS JOIN batch, acquire_lock
    ORDER BY batch.offset
    RETURNING mq.job_id
    INTO v_job_id;

    IF v_job_id IS NULL THEN
        RAISE EXCEPTION
            'missing lock sentinel for bucket %',
            v_bucket;
    END IF;

    RETURN v_job_id;
END;
$$;

UPSERT INTO fabmq_meta (key, value)
VALUES
    ('schema_version', '1'),
    ('product_version', '0.1.0');
