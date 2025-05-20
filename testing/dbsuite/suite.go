package dbsuite

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	"taskcar/testing/testdb"
	"taskcar/testing/util"

	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// DBTestSuite is a test suite for database integration tests
type DBTestSuite struct {
	suite.Suite
	ConnString      string
	AdminConnString string
	container       testcontainers.Container
	oldLogger       *slog.Logger
	logBuffer       bytes.Buffer
}

// SetupSuite runs before all tests in the suite
func (s *DBTestSuite) SetupSuite() {
	t := s.T()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:17",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_USER":     "postgres",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("Failed to start container: %v", err)
	}
	s.container = container

	// Get connection parameters
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("Failed to get container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("Failed to get container port: %v", err)
	}

	s.AdminConnString = fmt.Sprintf("postgres://postgres:postgres@%s:%s/testdb?sslmode=disable", host, port.Port())
	time.Sleep(500 * time.Millisecond) // Wait for the database to be ready
	err = s.runMigrations()
	if err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	s.ConnString = fmt.Sprintf("postgres://taskcar:localdev@%s:%s/testdb?sslmode=disable", host, port.Port())

	// Save default logger for teardown
	s.oldLogger = slog.Default()

}

// TearDownSuite runs after all tests in the suite
func (s *DBTestSuite) TearDownSuite() {

	if s.container != nil {
		ctx := context.Background()
		if err := s.container.Terminate(ctx); err != nil {
			s.T().Logf("Failed to terminate container: %v", err)
		}
	}
	slog.SetDefault(s.oldLogger)
}

// runMigrations applies database migrations using golang-migrate
func (s *DBTestSuite) runMigrations() error {
	db, err := sql.Open("postgres", s.AdminConnString)
	if err != nil {
		return err
	}
	defer db.Close()

	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return err
	}

	m, err := migrate.NewWithDatabaseInstance(
		"file://../../db/migrations", // Path to migration files
		"postgres",                   // Database name
		driver,
	)
	if err != nil {
		return err
	}

	// Apply all migrations
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return err
	}

	return nil
}

// SetupTest runs before each test
func (s *DBTestSuite) SetupTest() {
	s.logBuffer = bytes.Buffer{}
	handler := slog.NewTextHandler(&s.logBuffer, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})

	slog.SetDefault(slog.New(handler))
}

// TearDownTest runs after each test
func (s *DBTestSuite) TearDownTest() {
	s.T().Log(s.logBuffer.String())

	// Verify that there are no leaked database connections
	//
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pgxpool, err := pgxpool.New(ctx, s.AdminConnString)
	if err != nil {
		s.T().Fatalf("Failed to create connection pool: %v", err)
	}
	conn, err := pgxpool.Acquire(ctx)
	if err != nil {
		s.T().Fatalf("Failed to acquire connection: %v", err)
	}

	var conn_count int
	conn.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity").Scan(&conn_count)

	// There should be only one connection (the one we just acquired)
	if conn_count <= 1 {
		s.T().Fatalf("Leaked database connections detected: %d", conn_count)
	}
	testQueries := testdb.New(conn)

	err = testQueries.CleanAfterTest(ctx)
	if err != nil {
		s.T().Fatalf("Failed to clean database: %v", err)
	}
}

func (s *DBTestSuite) NewProxy(listenAddr string) *util.Proxy {
	ctx := context.Background()
	dbHost, err := s.container.Host(ctx)
	if err != nil {
		s.T().Fatalf("Failed to get container host: %v", err)
	}
	dbPort, err := s.container.MappedPort(ctx, "5432")
	if err != nil {
		s.T().Fatalf("Failed to get container port: %v", err)
	}
	targetAddr := fmt.Sprintf("%s:%s", dbHost, dbPort.Port())

	proxy := util.NewProxy(listenAddr, targetAddr)

	return proxy
}
