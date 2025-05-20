package db

import (
	"database/sql"
	"embed"
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"log/slog"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// migrateDb applies database migrations from embedded SQL files.
// It returns an error if migrations fail or if the schema is left in a dirty state.
func MigrateDb(connStr string) error {
	// Create a new database connection
	dbConn, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}
	defer func() {
		if err := dbConn.Close(); err != nil {
			slog.Error("Failed to close database connection", "error", err)
		}
	}()

	// Validate connection

	if err := dbConn.Ping(); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	// Create database driver
	dbDriver, err := postgres.WithInstance(dbConn, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("failed to create postges driver: %w", err)
	}

	// Create migration source from embedded FS
	d, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("failed to create migration source: %w", err)
	}

	// Create and use the migrate instance
	m, err := migrate.NewWithInstance("iofs", d, "postgres", dbDriver)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}

	// Set up logging
	m.Log = &migrationLogger{
		logger: slog.Default(),
	}

	// Run the migration
	slog.Info("Applying database migrations")
	if err := m.Up(); err != nil {
		if err == migrate.ErrNoChange {
			slog.Info("No migrations to apply")
		} else {
			return fmt.Errorf("failed to apply migrations: %w", err)
		}
	} else {
		slog.Info("Migrations applied successfully")
	}

	// Check version and state
	version, dirty, err := m.Version()
	if err != nil {
		if err != migrate.ErrNilVersion {
			return fmt.Errorf("failed to get migration version: %w", err)
		}
		slog.Info("No migrations have been applied yet")
	} else {
		slog.Info("Migration status", "version", version, "dirty", dirty)
		if dirty {
			return fmt.Errorf("database schema is in dirty state at version %d", version)
		}
	}

	return nil
}

// migrationLogger adapts slog to the migrate library's logger interface
type migrationLogger struct {
	logger *slog.Logger
}

func (l *migrationLogger) Printf(format string, v ...interface{}) {
	l.logger.Info(fmt.Sprintf(format, v...))
}

func (l *migrationLogger) Verbose() bool {
	return false
}
