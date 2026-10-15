package agent

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const addressPath = "/api/v1/server/machine/address"

type publicAddressProbe struct {
	sync.Mutex
	lastCheck time.Time
	running   bool
	values    map[string]any
}

// PublicAddresses never blocks resource reporting and only contacts the configured panel.
func (c *Client) PublicAddresses() map[string]any {
	c.addresses.Lock()
	result := make(map[string]any, len(c.addresses.values))
	for key, value := range c.addresses.values {
		result[key] = value
	}
	refresh := !c.addresses.running && time.Since(c.addresses.lastCheck) >= 5*time.Minute
	if refresh {
		c.addresses.running = true
		c.addresses.lastCheck = time.Now()
	}
	c.addresses.Unlock()
	if refresh {
		go c.refreshPublicAddresses()
	}
	return result
}

func (c *Client) refreshPublicAddresses() {
	values := map[string]any{}
	for _, family := range []string{"4", "6"} {
		transport := &http.Transport{
			// Ignore proxy environment variables so this is the host's direct egress.
			DialContext: func(ctx context.Context, _ string, address string) (net.Conn, error) {
				dialer := net.Dialer{Timeout: 3 * time.Second}
				return dialer.DialContext(ctx, "tcp"+family, address)
			},
			TLSHandshakeTimeout:    3 * time.Second,
			ResponseHeaderTimeout:  3 * time.Second,
			MaxResponseHeaderBytes: maxPanelResponseHeaderBytes,
		}
		probe := &Client{baseURL: c.baseURL, token: c.token, machineID: c.machineID, http: &http.Client{Timeout: 6 * time.Second, Transport: transport,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
		}}
		var response struct {
			Data struct {
				IP string `json:"ip"`
			} `json:"data"`
		}
		err := probe.Get(addressPath, nil, &response)
		transport.CloseIdleConnections()
		if err == nil {
			if ip := validPublicAddress(response.Data.IP, family); ip != "" {
				values["public_ipv"+family] = ip
			}
		}
	}
	c.addresses.Lock()
	c.addresses.values = values
	c.addresses.running = false
	c.addresses.Unlock()
}

func validPublicAddress(value, family string) string {
	ip := net.ParseIP(strings.TrimSpace(value))
	if ip == nil || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return ""
	}
	v4 := ip.To4()
	if family == "4" {
		if v4 == nil || v4[0] == 0 || v4[0] >= 224 || (v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127) || (v4[0] == 198 && (v4[1] == 18 || v4[1] == 19)) {
			return ""
		}
	} else if v4 != nil {
		return ""
	}
	return ip.String()
}
