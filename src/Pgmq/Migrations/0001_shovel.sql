-- Pgmq.NET's own routines live in the pgmq_dotnet schema, never in pgmq's, so pgmq stays exactly as upstream ships it.

-- shovel: moves messages from a queue, or from its archive, into another queue, in one statement.
--   source_queue  the queue to take messages from
--   target_queue  the queue to put them on; it must exist
--   from_archive  take them from the source's archive (pgmq.a_<source>) instead of the queue itself
--   max_messages  move at most this many, oldest first; NULL moves all of them
-- Each message keeps its body and headers and arrives through pgmq.send_batch as a new message, visible at once, with
-- a new msg_id and a read count of zero. From a queue only visible messages move: one being handled (its lease not yet
-- lapsed) or delayed stays where it is, and rows another transaction holds are skipped. Everything happens in the
-- caller's transaction, so the messages either all move or none do. Returns how many moved.
CREATE FUNCTION pgmq_dotnet.shovel(
    source_queue TEXT,
    target_queue TEXT,
    from_archive BOOLEAN DEFAULT false,
    max_messages INTEGER DEFAULT NULL
) RETURNS BIGINT AS $$
DECLARE
    source_table TEXT := pgmq.format_table_name(source_queue, CASE WHEN from_archive THEN 'a' ELSE 'q' END);
    msgs JSONB[];
    hdrs JSONB[];
BEGIN
    IF max_messages IS NOT NULL AND max_messages < 1 THEN
        RAISE EXCEPTION 'max_messages must be at least 1, or NULL to move every message';
    END IF;

    IF NOT from_archive AND lower(source_queue) = lower(target_queue) THEN
        RAISE EXCEPTION 'cannot shovel queue % into itself', source_queue;
    END IF;

    EXECUTE format(
        $QUERY$
        WITH taken AS (
            DELETE FROM pgmq.%I
            WHERE msg_id IN (
                SELECT msg_id FROM pgmq.%I
                %s
                ORDER BY msg_id
                LIMIT $1
                FOR UPDATE SKIP LOCKED)
            RETURNING msg_id, message, headers
        )
        SELECT array_agg(message ORDER BY msg_id), array_agg(headers ORDER BY msg_id) FROM taken
        $QUERY$,
        source_table, source_table,
        CASE WHEN from_archive THEN '' ELSE 'WHERE vt <= clock_timestamp()' END)
    INTO msgs, hdrs
    USING max_messages;

    IF msgs IS NULL THEN
        RETURN 0;
    END IF;

    PERFORM pgmq.send_batch(target_queue, msgs, hdrs);
    RETURN cardinality(msgs);
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;
