// Package migrations embeds the golang-migrate SQL files so they ship inside
// the binary. They are also the schema source for sqlc (see sqlc.yaml).
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
