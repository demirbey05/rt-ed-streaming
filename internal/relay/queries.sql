-- name: FetchUnpublished :many
-- Locks a batch of unpublished events. SKIP LOCKED lets several relay
-- instances run side by side: each one takes rows nobody else holds.
SELECT id, aggregate_type, aggregate_id, event_type, payload, created_at
FROM outbox
WHERE published_at IS NULL
ORDER BY created_at, id
LIMIT $1
FOR UPDATE SKIP LOCKED;

-- name: MarkPublished :exec
UPDATE outbox
SET published_at = now(),
    attempts     = attempts + 1,
    last_error   = NULL
WHERE id = ANY(@ids::uuid[]);

-- name: MarkFailed :exec
UPDATE outbox
SET attempts   = attempts + 1,
    last_error = @last_error
WHERE id = @id;

-- name: CountUnpublished :one
SELECT count(*) FROM outbox WHERE published_at IS NULL;
