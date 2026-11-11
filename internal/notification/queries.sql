-- name: MarkProcessed :execrows
-- 0 rows affected means this consumer already processed the message.
INSERT INTO processed_messages (consumer, message_id)
VALUES ($1, $2)
ON CONFLICT (consumer, message_id) DO NOTHING;

-- name: InsertNotification :exec
INSERT INTO notifications (id, event_id, order_id, customer_id, message)
VALUES ($1, $2, $3, $4, $5);

-- name: ListNotifications :many
SELECT * FROM notifications
WHERE customer_id = $1
ORDER BY created_at DESC
LIMIT $2;
