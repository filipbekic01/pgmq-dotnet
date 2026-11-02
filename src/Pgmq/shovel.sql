-- Pgmq.NET's one addition to pgmq, run by the migrator after pgmq is installed or upgraded. Idempotent.
-- Needs pgmq 1.5 or later: queue headers and pgmq.send_batch(text, jsonb[], jsonb[]) came in 1.5.0.

-- An earlier signature, without msg_ids, would make calls ambiguous.
DROP FUNCTION IF EXISTS pgmq.shovel(TEXT, TEXT, BOOLEAN, INTEGER);

-- shovel: moves messages from a queue, or from its archive, into another queue, in one statement.
--   source_queue  the queue to take messages from
--   target_queue  the queue to put them on; it must exist
--   from_archive  take them from the source's archive (pgmq.a_<source>) instead of the queue itself. They are moved,
--                 not copied: a message shoveled out of the archive is no longer in it.
--   max_messages  move at most this many, oldest first (1000 by default); NULL moves all of them in one transaction
--   msg_ids       move only these messages, whatever their number (max_messages does not apply). Ids that are not
--                 there, or not visible, are left out: the return value says how many moved
-- Each message keeps its body and headers and arrives through pgmq.send_batch as a new message, visible at once, with
-- a new msg_id and a read count of zero. From a queue only visible messages move: one being handled (its lease not yet
-- lapsed) or delayed stays where it is. Rows another transaction holds are skipped, so two shovels on one queue at once
-- never move the same message. The delete and the send run in the caller's transaction: if anything fails, nothing
-- moves. Returns how many moved; call it until it returns 0 to drain a queue in batches.
CREATE OR REPLACE FUNCTION pgmq.shovel(
    source_queue TEXT,
    target_queue TEXT,
    from_archive BOOLEAN DEFAULT false,
    max_messages INTEGER DEFAULT 1000,
    msg_ids BIGINT[] DEFAULT NULL
) RETURNS BIGINT AS $$
DECLARE
    source_table TEXT := pgmq.format_table_name(source_queue, CASE WHEN from_archive THEN 'a' ELSE 'q' END);
    msgs JSONB[];
    hdrs JSONB[];
    conditions TEXT[] := ARRAY[]::TEXT[];
BEGIN
    IF max_messages IS NOT NULL AND max_messages < 1 THEN
        RAISE EXCEPTION 'max_messages must be at least 1, or NULL to move every message';
    END IF;

    IF NOT from_archive AND lower(source_queue) = lower(target_queue) THEN
        RAISE EXCEPTION 'cannot shovel queue % into itself', source_queue;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pgmq.meta WHERE lower(queue_name) = lower(source_queue)) THEN
        RAISE EXCEPTION 'source queue % does not exist', source_queue;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pgmq.meta WHERE lower(queue_name) = lower(target_queue)) THEN
        RAISE EXCEPTION 'target queue % does not exist', target_queue;
    END IF;

    IF NOT from_archive THEN
        conditions := array_append(conditions, 'vt <= clock_timestamp()');
    END IF;

    IF msg_ids IS NOT NULL THEN
        conditions := array_append(conditions, 'msg_id = ANY ($2)');
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
        CASE WHEN cardinality(conditions) = 0 THEN '' ELSE 'WHERE ' || array_to_string(conditions, ' AND ') END)
    INTO msgs, hdrs
    USING CASE WHEN msg_ids IS NULL THEN max_messages END, msg_ids;

    IF msgs IS NULL THEN
        RETURN 0;
    END IF;

    PERFORM pgmq.send_batch(target_queue, msgs, hdrs);
    RETURN cardinality(msgs);
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;
