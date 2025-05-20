package util

import (
	"io"
	"log"
	"net"
	"sync"
)

// Proxy represents a TCP proxy
type Proxy struct {
	ListenAddr string
	TargetAddr string
	listener   net.Listener
	conns      []net.Conn
	mu         sync.Mutex
	done       chan struct{}
}

// NewProxy creates a new TCP proxy
func NewProxy(listenAddr, targetAddr string) *Proxy {
	return &Proxy{
		ListenAddr: listenAddr,
		TargetAddr: targetAddr,
		conns:      make([]net.Conn, 0),
		done:       make(chan struct{}),
	}
}

// Start begins accepting connections
func (p *Proxy) Start() error {
	listener, err := net.Listen("tcp", p.ListenAddr)
	if err != nil {
		return err
	}
	p.listener = listener

	go p.acceptLoop()
	return nil
}

func (p *Proxy) acceptLoop() {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			select {
			case <-p.done:
				return // Normal shutdown
			default:
				log.Printf("Accept error: %v", err)
				continue
			}
		}

		go p.handleConn(conn)
	}
}

func (p *Proxy) handleConn(conn net.Conn) {
	defer conn.Close()

	// Connect to target
	target, err := net.Dial("tcp", p.TargetAddr)
	if err != nil {
		log.Printf("Target connection error: %v", err)
		return
	}
	defer target.Close()

	// Register connections
	p.mu.Lock()
	p.conns = append(p.conns, conn, target)
	p.mu.Unlock()

	// Cleanup on exit
	defer func() {
		p.mu.Lock()
		for i := len(p.conns) - 1; i >= 0; i-- {
			if p.conns[i] == conn || p.conns[i] == target {
				p.conns = append(p.conns[:i], p.conns[i+1:]...)
			}
		}
		p.mu.Unlock()
	}()

	// Copy in both directions
	errCh := make(chan error, 2)
	go func() {
		_, err := io.Copy(target, conn)
		errCh <- err
	}()
	go func() {
		_, err := io.Copy(conn, target)
		errCh <- err
	}()

	// Wait for either direction to fail
	<-errCh
}

func (p *Proxy) DisconnectAll() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, conn := range p.conns {
		conn.Close()
	}
	p.conns = p.conns[:0]
}

func (p *Proxy) Stop() error {
	close(p.done)
	if p.listener != nil {
		return p.listener.Close()
	}
	return nil
}
