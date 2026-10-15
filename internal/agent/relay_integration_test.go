package agent

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// CI supplies the real, checksum-verified GOST release executable.
func TestGOSTForwarding(t *testing.T) {
	binary := os.Getenv("RAVEL_TEST_GOST")
	if binary == "" {
		t.Skip("RAVEL_TEST_GOST is not configured")
	}
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer tcp.Close()
	go func() {
		for {
			conn, err := tcp.Accept()
			if err != nil {
				return
			}
			go func() { defer conn.Close(); _, _ = io.Copy(conn, conn) }()
		}
	}()
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	go func() {
		data := make([]byte, 1024)
		for {
			n, peer, err := udp.ReadFrom(data)
			if err != nil {
				return
			}
			_, _ = udp.WriteTo(data[:n], peer)
		}
	}()
	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := free.Addr().(*net.TCPAddr).Port
	free.Close()
	manager := NewRelayManager(binary, filepath.Join(t.TempDir(), "relay.json"))
	defer manager.Shutdown()
	_, err = manager.SyncContext(context.Background(), RelayConfig{Rules: []RelayRule{
		{ListenHost: "127.0.0.1", ListenPort: flexiblePort(port), TargetHost: "127.0.0.1", TargetPort: flexiblePort(tcp.Addr().(*net.TCPAddr).Port), Protocol: "tcp"},
		{ListenHost: "127.0.0.1", ListenPort: flexiblePort(port), TargetHost: "127.0.0.1", TargetPort: flexiblePort(udp.LocalAddr().(*net.UDPAddr).Port), Protocol: "udp"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, protocol := range []string{"tcp", "udp"} {
		conn, err := net.DialTimeout(protocol, net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write([]byte("ravel-gost")); err != nil {
			conn.Close()
			t.Fatal(err)
		}
		data := make([]byte, len("ravel-gost"))
		_, err = io.ReadFull(conn, data)
		conn.Close()
		if err != nil || string(data) != "ravel-gost" {
			t.Fatalf("%s forwarding: %q, %v", protocol, data, err)
		}
	}
}
