package integrationtest_test

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"taskcar/listener"
)

func (s *IntTestSuite) TestListenerRecievesMessages() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	pool, err := pgxpool.New(ctx, s.ConnString)
	defer pool.Close()

	chans := listener.ListenQueues([]string{"test1", "test2"}, s.ConnString, ctx)
	defer cancel()

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Failed to acquire connection: %v", err)
	}
	defer conn.Release()

	//_, err = conn.Exec(context.Background(), "NOTIFY differentqueue, ''")
	// Wait for the channl to listned to before testing notification
	message := <-chans["test1"]
	assert.Equal(t, listener.Message{Queue: "test1", Data: "Connected"}, message, "Expected message not received")

	_, err = conn.Exec(context.Background(), "NOTIFY test1, ''")

	message = <-chans["test1"]
	assert.Equal(t, listener.Message{Queue: "test1", Data: ""}, message, "Expected message not received")

	message = <-chans["test2"]
	assert.Equal(t, listener.Message{Queue: "test2", Data: "Connected"}, message, "Expected message not received")

	_, err = conn.Exec(context.Background(), "NOTIFY test2, 'test2'")
	message = <-chans["test2"]
	assert.Equal(t, listener.Message{Queue: "test2", Data: "test2"}, message, "Expected message not received")

}

func (s *IntTestSuite) TestListenerNetworkInterruption() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())

	proxy := s.NewProxy("localhost:55432")

	proxiedConnString := "postgres://taskcar:localdev@localhost:55432/testdb?sslmode=disable"

	proxy.Start()
	pool, err := pgxpool.New(ctx, s.ConnString)
	defer pool.Close()

	chans := listener.ListenQueues([]string{"test1"}, proxiedConnString, ctx)
	defer cancel()

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Failed to acquire connection: %v", err)
	}
	defer conn.Release()

	message := <-chans["test1"]
	assert.Equal(t, listener.Message{Queue: "test1", Data: "Connected"}, message, "Expected message not received")

	_, err = conn.Exec(context.Background(), "NOTIFY test1, '1'")
	if err != nil {
		t.Fatalf("Could not send notification 1 error: %v", err)
	}

	message = <-chans["test1"]
	assert.Equal(t, listener.Message{Queue: "test1", Data: "1"}, message, "Expected message not received")

	proxy.DisconnectAll()

	message = <-chans["test1"]
	assert.Equal(t, listener.Message{Queue: "test1", Data: "Connected"}, message, "Expected message not received")

	_, err = conn.Exec(context.Background(), "NOTIFY test1, '2'")
	if err != nil {
		t.Fatalf("Could not send notification 2 error: %v", err)
	}

	message = <-chans["test1"]
	assert.Equal(t, listener.Message{Queue: "test1", Data: "2"}, message, "Expected message not received")
}
