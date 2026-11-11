package db

import (
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // registers the pgx5:// driver
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"rt-ed-streaming/migrations"
)

// MigrateUp applies all pending migrations from the embedded files.
func MigrateUp(databaseURL string) error {
	return withMigrate(databaseURL, func(m *migrate.Migrate) error {
		if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return err
		}
		return nil
	})
}

// MigrateDown rolls back every migration. Intended for tests.
func MigrateDown(databaseURL string) error {
	return withMigrate(databaseURL, func(m *migrate.Migrate) error {
		if err := m.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return err
		}
		return nil
	})
}

func withMigrate(databaseURL string, fn func(*migrate.Migrate) error) error {
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("migrations source: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, toPgx5URL(databaseURL))
	if err != nil {
		return fmt.Errorf("init migrate: %w", err)
	}
	defer m.Close()

	if err := fn(m); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}

// golang-migrate selects its driver by URL scheme; the pgx v5 driver uses "pgx5".
func toPgx5URL(url string) string {
	for _, prefix := range []string{"postgres://", "postgresql://"} {
		if rest, ok := strings.CutPrefix(url, prefix); ok {
			return "pgx5://" + rest
		}
	}
	return url
}
