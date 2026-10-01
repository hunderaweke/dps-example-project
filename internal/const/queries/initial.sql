-- name: CreateExample :one
INSERT INTO examples (id, name, description, owner_id, status)
VALUES ($1, $2, $3, $4, $5)
RETURNING *;

-- name: GetExample :one
SELECT * FROM examples
WHERE id = $1;

-- name: ListExamples :many
SELECT * FROM examples
ORDER BY created_at DESC
LIMIT $1 OFFSET $2;

-- name: UpdateExampleStatus :one
UPDATE examples
SET status = $2, updated_at = now()
WHERE id = $1
RETURNING *;
