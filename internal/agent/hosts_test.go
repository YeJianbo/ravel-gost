package agent

import "testing"

func TestNetworkHostRejectsInjectedOptions(t *testing.T) {
	for _, host := range []string{"example.com/path", "localhost?x=1", "user@host", "-f", "host\nvalue", "host%2fpath", ""} {
		if validNetworkHost(host) {
			t.Fatalf("accepted %q", host)
		}
	}
	for _, host := range []string{"example.com", "127.0.0.1", "::1", "[2001:db8::1]", "localhost"} {
		if !validNetworkHost(host) {
			t.Fatalf("rejected %q", host)
		}
	}
}
