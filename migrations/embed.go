// Package migrations embeds the SQL migration files so they can be applied
// from Go (integration tests, and later on service startup).
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
