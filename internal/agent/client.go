package agent

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const protocol = "ravel-v1"
const maxPanelResponseHeaderBytes = 64 << 10

type State struct {
	PanelURL       string `json:"panel_url"`
	MachineToken   string `json:"machine_token"`
	MachineID      int    `json:"machine_id"`
	Protocol       string `json:"protocol"`
	SyncInterval   int    `json:"sync_interval"`
	StatusInterval int    `json:"status_interval"`
}

type Client struct {
	baseURL   string
	token     string
	machineID int
	http      *http.Client
	addresses publicAddressProbe
}

func NewClient(state State) (*Client, error) {
	if strings.TrimSpace(state.PanelURL) == "" || strings.TrimSpace(state.MachineToken) == "" || state.MachineID <= 0 {
		return nil, fmt.Errorf("incomplete ravel state")
	}
	if state.Protocol != "" && state.Protocol != protocol {
		return nil, fmt.Errorf("unsupported protocol %q", state.Protocol)
	}
	panel, err := url.Parse(state.PanelURL)
	if err != nil || panel.Host == "" || panel.User != nil || panel.RawQuery != "" || panel.Fragment != "" {
		return nil, fmt.Errorf("invalid panel URL")
	}
	if panel.Scheme != "https" && !(panel.Scheme == "http" && (panel.Hostname() == "127.0.0.1" || panel.Hostname() == "::1" || panel.Hostname() == "localhost")) {
		return nil, fmt.Errorf("panel URL requires HTTPS")
	}
	return &Client{
		baseURL:   strings.TrimRight(state.PanelURL, "/"),
		token:     state.MachineToken,
		machineID: state.MachineID,
		http: &http.Client{
			Timeout:       30 * time.Second,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
			Transport: &http.Transport{
				Proxy:                  http.ProxyFromEnvironment,
				MaxIdleConns:           8,
				MaxIdleConnsPerHost:    4,
				IdleConnTimeout:        90 * time.Second,
				TLSHandshakeTimeout:    10 * time.Second,
				ResponseHeaderTimeout:  20 * time.Second,
				MaxResponseHeaderBytes: maxPanelResponseHeaderBytes,
			},
		},
	}, nil
}

func (c *Client) Get(path string, query url.Values, out any) error {
	return c.GetContext(context.Background(), path, query, out)
}

func (c *Client) GetContext(ctx context.Context, path string, query url.Values, out any) error {
	return c.do(ctx, http.MethodGet, path, query, nil, out)
}

func (c *Client) Post(path string, body any, out any) error {
	return c.PostContext(context.Background(), path, body, out)
}

func (c *Client) PostContext(ctx context.Context, path string, body any, out any) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	return c.do(ctx, http.MethodPost, path, nil, raw, out)
}

func (c *Client) do(ctx context.Context, method, path string, query url.Values, body []byte, out any) error {
	if ctx == nil {
		return fmt.Errorf("nil request context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	rawQuery := ""
	if query != nil {
		rawQuery = query.Encode()
	}
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	nonceBytes := make([]byte, 16)
	if _, err := rand.Read(nonceBytes); err != nil {
		return fmt.Errorf("generate nonce: %w", err)
	}
	nonce := hex.EncodeToString(nonceBytes)
	bodyDigest := sha256.Sum256(body)
	payload := strings.Join([]string{method, path, rawQuery, timestamp, nonce, hex.EncodeToString(bodyDigest[:])}, "\n")
	mac := hmac.New(sha256.New, []byte(c.token))
	_, _ = mac.Write([]byte(payload))
	signature := hex.EncodeToString(mac.Sum(nil))

	requestURL := c.baseURL + path
	if rawQuery != "" {
		requestURL += "?" + rawQuery
	}
	req, err := http.NewRequestWithContext(ctx, method, requestURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "ravel")
	req.Header.Set("Accept", "application/json")
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("X-Ravel-Machine-Id", strconv.Itoa(c.machineID))
	req.Header.Set("X-Ravel-Timestamp", timestamp)
	req.Header.Set("X-Ravel-Nonce", nonce)
	req.Header.Set("X-Ravel-Signature", signature)

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("panel returned HTTP %d", resp.StatusCode)
	}
	maxResponseBytes, responseLimit := panelResponseLimit(path)
	if resp.ContentLength > maxResponseBytes {
		return fmt.Errorf("panel response exceeds %s", responseLimit)
	}
	if out == nil {
		n, err := io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes+1))
		if err != nil {
			return err
		}
		if n > maxResponseBytes {
			return fmt.Errorf("panel response exceeds %s", responseLimit)
		}
		return nil
	}
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return err
	}
	if int64(len(responseBody)) > maxResponseBytes {
		return fmt.Errorf("panel response exceeds %s", responseLimit)
	}
	if len(responseBody) > 0 {
		if err := json.Unmarshal(responseBody, out); err != nil {
			return fmt.Errorf("decode panel response: %w", err)
		}
	}
	return nil
}

func panelResponseLimit(path string) (int64, string) {
	switch path {
	case statusPath, updateAckPath, networkQualityPath, addressPath:
		return 128 << 10, "128 KiB"
	default:
		return 8 << 20, "8 MiB"
	}
}
