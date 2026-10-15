package agent

import "testing"

func TestPublicAddressValidatesActualFamilyAndRejectsPrivateIPs(t *testing.T) {
	for _, item := range []struct{ ip, family, want string }{
		{"8.8.8.8", "4", "8.8.8.8"},
		{"2001:4860:4860::8888", "6", "2001:4860:4860::8888"},
		{"8.8.8.8", "6", ""},
		{"2001:4860:4860::8888", "4", ""},
		{"10.0.0.1", "4", ""},
		{"100.64.0.1", "4", ""},
		{"198.18.0.1", "4", ""},
		{"fc00::1", "6", ""},
		{"not-an-ip", "4", ""},
	} {
		if got := validPublicAddress(item.ip, item.family); got != item.want {
			t.Fatalf("%s family %s: got %q, want %q", item.ip, item.family, got, item.want)
		}
	}
}
