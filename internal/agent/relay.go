package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	relayTargetProbeInterval = 30 * time.Second
	relayTargetProbeTimeout  = 2 * time.Second
)

type flexiblePort int

func (p *flexiblePort) UnmarshalJSON(raw []byte) error {
	var number int
	if json.Unmarshal(raw, &number) == nil {
		*p = flexiblePort(number)
		return nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return err
	}
	value, err := strconv.Atoi(text)
	if err != nil {
		return err
	}
	*p = flexiblePort(value)
	return nil
}

type flexibleStrings []string

func (s *flexibleStrings) UnmarshalJSON(raw []byte) error {
	var values []string
	if json.Unmarshal(raw, &values) == nil {
		*s = values
		return nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	*s = strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ';' || r == '/' || r == '|' || r == '+' || r == ' '
	})
	return nil
}

type RelayConfig struct {
	Rules []RelayRule `json:"rules"`
}

type RelayRule struct {
	ListenHost    string          `json:"listen_host"`
	ListenHostAlt string          `json:"listenHost"`
	LocalHost     string          `json:"local_host"`
	LocalHostAlt  string          `json:"localHost"`
	ListenPort    flexiblePort    `json:"listen_port"`
	ListenPortAlt flexiblePort    `json:"listenPort"`
	LocalPort     flexiblePort    `json:"local_port"`
	LocalPortAlt  flexiblePort    `json:"localPort"`
	TargetHost    string          `json:"target_host"`
	TargetHostAlt string          `json:"targetHost"`
	RemoteHost    string          `json:"remote_host"`
	RemoteHostAlt string          `json:"remoteHost"`
	Host          string          `json:"host"`
	TargetPort    flexiblePort    `json:"target_port"`
	TargetPortAlt flexiblePort    `json:"targetPort"`
	RemotePort    flexiblePort    `json:"remote_port"`
	RemotePortAlt flexiblePort    `json:"remotePort"`
	Port          flexiblePort    `json:"port"`
	Protocols     flexibleStrings `json:"protocols"`
	Protocol      string          `json:"protocol"`
	Type          string          `json:"type"`
}

type gostConfig struct {
	Debug      bool     `json:"Debug"`
	Retries    int      `json:"Retries"`
	ServeNodes []string `json:"ServeNodes"`
}

type relayListener struct {
	Protocol   string
	ListenHost string
	ListenPort int
	TargetHost string
	TargetPort int
}

type relayTargetHealth struct {
	LatencyMs *float64
	Status    string
	Message   string
	CheckedAt int64
}

type RelayRuleHealth struct {
	TargetHost      string   `json:"target_host,omitempty"`
	TargetPort      int      `json:"target_port,omitempty"`
	TargetLatencyMs *float64 `json:"target_latency_ms,omitempty"`
	Protocol        string   `json:"protocol"`
	ListenHost      string   `json:"listen_host"`
	ListenPort      int      `json:"listen_port"`
	Status          string   `json:"status"`
	ListenOK        bool     `json:"listen_ok"`
	Message         string   `json:"message"`
	CheckedAt       int64    `json:"checked_at"`
	TargetStatus    string   `json:"target_status,omitempty"`
	TargetOK        *bool    `json:"target_ok,omitempty"`
	TargetMessage   string   `json:"target_message,omitempty"`
	TargetCheckedAt int64    `json:"target_checked_at,omitempty"`
}

type RelayManager struct {
	BinaryPath string
	ConfigPath string

	operationMu        sync.Mutex
	mu                 sync.Mutex
	cmd                *exec.Cmd
	done               chan struct{}
	version            string
	versionFileSize    int64
	versionFileTime    int64
	ruleCount          int
	lastError          string
	paused             bool
	listeners          []relayListener
	targetHealth       map[string]relayTargetHealth
	targetProbeAt      time.Time
	targetProbeRunning bool
	targetProbeCancel  context.CancelFunc
	targetProbeEpoch   uint64
}

func NewRelayManager(binaryPath, configPath string) *RelayManager {
	return &RelayManager{
		BinaryPath:   binaryPath,
		ConfigPath:   configPath,
		targetHealth: make(map[string]relayTargetHealth),
	}
}

func (m *RelayManager) Sync(config RelayConfig) (bool, error) {
	return m.SyncContext(context.Background(), config)
}

func (m *RelayManager) SyncContext(ctx context.Context, config RelayConfig) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if len(config.Rules) > 1024 {
		return false, fmt.Errorf("relay configuration exceeds 1024 rules")
	}
	for _, rule := range config.Rules {
		listen := firstString(rule.ListenHost, rule.ListenHostAlt, rule.LocalHost, rule.LocalHostAlt, "0.0.0.0")
		target := firstString(rule.TargetHost, rule.TargetHostAlt, rule.RemoteHost, rule.RemoteHostAlt, rule.Host)
		if !validNetworkHost(listen) || !validNetworkHost(target) {
			return false, fmt.Errorf("relay address must be an IP or hostname without URL options")
		}
	}
	m.operationMu.Lock()
	defer m.operationMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}

	if runtime.GOOS != "linux" {
		m.lastError = "relay management is only supported on Linux"
		return false, fmt.Errorf("%s", m.lastError)
	}
	nodes := normalizeRelayRules(config.Rules)
	if len(nodes) == 0 {
		changed := m.cmd != nil
		m.stopLocked()
		if err := os.Remove(m.ConfigPath); err == nil {
			changed = true
		}
		m.ruleCount = 0
		m.listeners = nil
		m.targetHealth = make(map[string]relayTargetHealth)
		m.lastError = ""
		return changed, nil
	}
	if !m.paused {
		if err := m.prepareBinaryOutsideStateLockContext(ctx); err != nil {
			m.lastError = err.Error()
			return false, err
		}
	}

	raw, err := json.MarshalIndent(gostConfig{ServeNodes: nodes}, "", "  ")
	if err != nil {
		m.lastError = err.Error()
		return false, err
	}
	raw = append(raw, '\n')
	if len(raw) > maxAgentConfigFileBytes {
		err := fmt.Errorf("relay configuration exceeds %d bytes", maxAgentConfigFileBytes)
		m.lastError = err.Error()
		return false, err
	}
	current, err := readBoundedAgentFile(m.ConfigPath, maxAgentConfigFileBytes)
	if err != nil && !os.IsNotExist(err) {
		err = fmt.Errorf("read existing relay configuration: %w", err)
		m.lastError = err.Error()
		return false, err
	}
	changed := !bytes.Equal(current, raw)
	if changed {
		if err := atomicWrite(m.ConfigPath, raw, 0o600); err != nil {
			m.lastError = err.Error()
			return false, err
		}
		m.stopLocked()
	}
	if m.paused {
		m.ruleCount = len(nodes)
		m.setRelayListenersLocked(relayListenersFromNodes(nodes))
		m.lastError = ""
		return changed, nil
	}
	if err := ctx.Err(); err != nil {
		return changed, err
	}
	if m.cmd == nil {
		if err := m.startAndVerifyLocked(); err != nil {
			if changed {
				m.restoreRelayConfigLocked(current)
			}
			m.lastError = err.Error()
			return changed, err
		}
	}
	m.ruleCount = len(nodes)
	m.setRelayListenersLocked(relayListenersFromNodes(nodes))
	m.lastError = ""
	return changed, nil
}

func (m *RelayManager) StartExisting() error {
	return m.StartExistingContext(context.Background())
}

func (m *RelayManager) StartExistingContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.operationMu.Lock()
	defer m.operationMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.paused {
		return nil
	}
	return m.startExistingLocked(ctx)
}

func (m *RelayManager) startExistingLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if m.cmd != nil && m.cmd.Process != nil {
		if m.done == nil {
			return nil
		}
		select {
		case <-m.done:
			m.cmd = nil
			m.done = nil
		default:
			return nil
		}
	}

	raw, err := readBoundedAgentFile(m.ConfigPath, maxAgentConfigFileBytes)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var config gostConfig
	if err := json.Unmarshal(raw, &config); err != nil {
		return fmt.Errorf("decode existing relay configuration: %w", err)
	}
	if len(config.ServeNodes) == 0 {
		return nil
	}
	if err := m.prepareBinaryOutsideStateLockContext(ctx); err != nil {
		m.lastError = err.Error()
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.startAndVerifyLocked(); err != nil {
		m.lastError = err.Error()
		return err
	}
	m.ruleCount = len(config.ServeNodes)
	m.setRelayListenersLocked(relayListenersFromNodes(config.ServeNodes))
	m.lastError = ""
	return nil
}

func (m *RelayManager) ControlContext(ctx context.Context, action string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.operationMu.Lock()
	defer m.operationMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	switch action {
	case "stop":
		// Keep config sync from undoing an explicit stop during this controller lifetime.
		m.paused = true
		m.stopLocked()
		m.lastError = ""
		return nil
	case "start", "restart", "reload":
		if action != "start" {
			m.stopLocked()
		}
		m.paused = false
		if err := m.startExistingLocked(ctx); err != nil {
			m.lastError = err.Error()
			return err
		}
		m.lastError = ""
		if m.cmd == nil {
			return fmt.Errorf("no synchronized forwarding rules available to start")
		}
		return nil
	default:
		return fmt.Errorf("unsupported managed relay action %q", action)
	}
}

func (m *RelayManager) Status() map[string]any {
	m.mu.Lock()
	status := m.statusLocked()
	m.scheduleTargetProbesLocked(status)
	result := m.statusFieldsLocked(status)
	cmd, epoch := m.cmd, m.targetProbeEpoch
	listeners := slices.Clone(m.listeners)
	listening := map[string]bool{}
	health := m.ruleHealthLocked(status, listening)
	external := status == "active" && cmd != nil && cmd.Process != nil && len(listeners) > 0
	m.mu.Unlock()

	if external {
		listening = processListeningSockets(cmd.Process.Pid, listeners)
		m.mu.Lock()
		if m.cmd != cmd || m.targetProbeEpoch != epoch {
			// A restart or rule change invalidates this process/listener snapshot.
			result = m.statusFieldsLocked(m.statusLocked())
			health = []RelayRuleHealth{}
		} else {
			for index, listener := range listeners {
				record := &health[index]
				record.ListenOK = listening[listener.Protocol+":"+strconv.Itoa(listener.ListenPort)]
				if record.ListenOK {
					record.Status = "running"
					record.Message = ""
				}
			}
		}
		m.mu.Unlock()
	}
	result["gost_rule_health"] = health
	return result
}

func (m *RelayManager) statusLocked() string {
	status := "inactive"
	if m.cmd != nil && m.cmd.Process != nil {
		status = "active"
	} else if m.lastError != "" {
		status = "failed"
	}
	return status
}

func (m *RelayManager) statusFieldsLocked(status string) map[string]any {
	return map[string]any{
		"gost_status":     status,
		"gost_version":    m.version,
		"gost_rule_count": m.ruleCount,
		"gost_error":      m.lastError,
	}
}

func (m *RelayManager) ruleHealthLocked(status string, listening map[string]bool) []RelayRuleHealth {
	if len(m.listeners) == 0 {
		return []RelayRuleHealth{}
	}
	checkedAt := time.Now().Unix()
	health := make([]RelayRuleHealth, 0, len(m.listeners))
	for _, listener := range m.listeners {
		listenOK := listening[listener.Protocol+":"+strconv.Itoa(listener.ListenPort)]
		message := ""
		ruleStatus := "running"
		if status != "active" {
			ruleStatus = status
			message = m.lastError
		} else if !listenOK {
			ruleStatus = "failed"
			message = "GOST 进程未持有该监听端口"
		}
		record := RelayRuleHealth{
			Protocol:   listener.Protocol,
			TargetHost: listener.TargetHost,
			TargetPort: listener.TargetPort,
			ListenHost: listener.ListenHost,
			ListenPort: listener.ListenPort,
			Status:     ruleStatus,
			ListenOK:   listenOK,
			Message:    message,
			CheckedAt:  checkedAt,
		}
		if listener.Protocol != "tcp" || listener.TargetHost == "" || listener.TargetPort <= 0 {
			record.TargetStatus = "not_applicable"
		} else if target, ok := m.targetHealth[relayTargetHealthKey(listener)]; ok {
			targetOK := target.Status == "reachable"
			record.TargetStatus = target.Status
			record.TargetOK = &targetOK
			record.TargetMessage = target.Message
			record.TargetCheckedAt = target.CheckedAt
			record.TargetLatencyMs = target.LatencyMs
		} else {
			record.TargetStatus = "pending"
		}
		health = append(health, record)
	}
	return health
}

func (m *RelayManager) scheduleTargetProbesLocked(status string) {
	if status != "active" || m.targetProbeRunning {
		return
	}
	now := time.Now()
	if !m.targetProbeAt.IsZero() && now.Sub(m.targetProbeAt) < relayTargetProbeInterval {
		return
	}
	listeners := make([]relayListener, 0, len(m.listeners))
	for _, listener := range m.listeners {
		if listener.Protocol == "tcp" && listener.TargetHost != "" && listener.TargetPort > 0 {
			listeners = append(listeners, listener)
		}
	}
	if len(listeners) == 0 {
		return
	}
	m.targetProbeAt = now
	m.targetProbeRunning = true
	ctx, cancel := context.WithCancel(context.Background())
	m.targetProbeCancel = cancel
	epoch := m.targetProbeEpoch
	go func() {
		defer cancel()
		results := probeRelayTargets(ctx, listeners)
		m.mu.Lock()
		defer m.mu.Unlock()
		if epoch != m.targetProbeEpoch {
			return
		}
		if ctx.Err() == nil {
			m.retainCurrentTargetHealthLocked(results)
		}
		m.targetProbeRunning = false
		m.targetProbeCancel = nil
	}()
}

func (m *RelayManager) cancelTargetProbesLocked() {
	m.targetProbeEpoch++
	if m.targetProbeCancel != nil {
		m.targetProbeCancel()
		m.targetProbeCancel = nil
	}
	m.targetProbeRunning = false
	m.targetProbeAt = time.Time{}
}

func (m *RelayManager) setRelayListenersLocked(listeners []relayListener) {
	if !slices.Equal(m.listeners, listeners) {
		m.cancelTargetProbesLocked()
	}
	m.listeners = listeners
	m.retainCurrentTargetHealthLocked(nil)
}

func (m *RelayManager) retainCurrentTargetHealthLocked(results map[string]relayTargetHealth) {
	current := make(map[string]relayTargetHealth, len(m.listeners))
	for _, listener := range m.listeners {
		if listener.Protocol != "tcp" || listener.TargetHost == "" || listener.TargetPort <= 0 {
			continue
		}
		key := relayTargetHealthKey(listener)
		result, ok := results[key]
		if !ok {
			result, ok = m.targetHealth[key]
		}
		if ok {
			current[key] = result
		}
	}
	m.targetHealth = current
}

func probeRelayTargets(ctx context.Context, listeners []relayListener) map[string]relayTargetHealth {
	if ctx.Err() != nil {
		return nil
	}
	results := make(map[string]relayTargetHealth, len(listeners))
	type targetJob struct {
		listener relayListener
		keys     []string
	}
	jobs := make([]targetJob, 0, len(listeners))
	byTarget := make(map[string]int, len(listeners))
	for _, listener := range listeners {
		if ctx.Err() != nil {
			return results
		}
		endpoint := net.JoinHostPort(strings.Trim(listener.TargetHost, "[]"), strconv.Itoa(listener.TargetPort))
		index, ok := byTarget[endpoint]
		if !ok {
			index = len(jobs)
			byTarget[endpoint] = index
			jobs = append(jobs, targetJob{listener: listener})
		}
		jobs[index].keys = append(jobs[index].keys, relayTargetHealthKey(listener))
	}
	var resultMu sync.Mutex
	var group sync.WaitGroup
	queue := make(chan targetJob)
	// Bound worker stacks as well as connections; multiple entrances share one target probe.
	for worker := 0; worker < min(8, len(jobs)); worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for job := range queue {
				if ctx.Err() != nil {
					return
				}
				result := probeRelayTargetContext(ctx, job.listener, relayTargetProbeTimeout)
				resultMu.Lock()
				for _, key := range job.keys {
					results[key] = result
				}
				resultMu.Unlock()
			}
		}()
	}
sendJobs:
	for _, job := range jobs {
		select {
		case queue <- job:
		case <-ctx.Done():
			break sendJobs
		}
	}
	close(queue)
	group.Wait()
	return results
}

func probeRelayTarget(listener relayListener, timeout time.Duration) relayTargetHealth {
	return probeRelayTargetContext(context.Background(), listener, timeout)
}

func probeRelayTargetContext(ctx context.Context, listener relayListener, timeout time.Duration) relayTargetHealth {
	result := relayTargetHealth{Status: "unreachable", CheckedAt: time.Now().Unix()}
	started := time.Now()
	dialer := net.Dialer{Timeout: timeout}
	connection, err := dialer.DialContext(
		ctx,
		"tcp",
		net.JoinHostPort(strings.Trim(listener.TargetHost, "[]"), strconv.Itoa(listener.TargetPort)),
	)
	if err != nil {
		result.Message = err.Error()
		return result
	}
	_ = connection.Close()
	elapsed := float64(time.Since(started).Microseconds()) / 1000
	result.LatencyMs = &elapsed
	result.Status = "reachable"
	result.CheckedAt = time.Now().Unix()
	return result
}

func relayTargetHealthKey(listener relayListener) string {
	return strings.Join([]string{
		listener.Protocol,
		strings.ToLower(strings.Trim(listener.ListenHost, "[]")),
		strconv.Itoa(listener.ListenPort),
		strings.ToLower(strings.Trim(listener.TargetHost, "[]")),
		strconv.Itoa(listener.TargetPort),
	}, "|")
}

// The caller holds operationMu, so lifecycle changes remain serialized while status reads continue.
func (m *RelayManager) prepareBinaryOutsideStateLockContext(ctx context.Context) error {
	m.mu.Unlock()
	defer m.mu.Lock()
	return m.ensureBinaryContext(ctx)
}

func (m *RelayManager) ensureBinaryContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := os.Stat(m.BinaryPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
		return fmt.Errorf("GOST executable missing; install the complete Ravel release bundle")
	}
	m.readVersionContext(ctx)
	return ctx.Err()
}

func (m *RelayManager) readVersionContext(ctx context.Context) {
	info, statErr := os.Stat(m.BinaryPath)
	output, _, err := runRuntimeCommandContext(ctx, 5*time.Second, m.BinaryPath, "-V")
	if err == nil && statErr == nil && strings.TrimSpace(output) != "" {
		m.mu.Lock()
		m.version = strings.TrimSpace(output)
		m.versionFileSize = info.Size()
		m.versionFileTime = info.ModTime().UnixNano()
		m.mu.Unlock()
	}
}

func (m *RelayManager) startLocked() error {
	cmd := exec.Command(m.BinaryPath, "-C", m.ConfigPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start GOST: %w", err)
	}
	done := make(chan struct{})
	m.cmd = cmd
	m.done = done
	go func() {
		err := cmd.Wait()
		close(done)
		m.mu.Lock()
		if m.cmd == cmd {
			m.cmd = nil
			m.done = nil
			if err != nil {
				m.lastError = err.Error()
			}
		}
		m.mu.Unlock()
	}()
	return nil
}

func (m *RelayManager) startAndVerifyLocked() error {
	if err := m.startLocked(); err != nil {
		return err
	}
	done := m.done
	if done == nil {
		return fmt.Errorf("GOST did not expose a process handle")
	}
	select {
	case <-done:
		m.cmd = nil
		m.done = nil
		return fmt.Errorf("GOST exited immediately; check relay ports and configuration")
	case <-time.After(500 * time.Millisecond):
		return nil
	}
}

func (m *RelayManager) restoreRelayConfigLocked(previous []byte) {
	m.stopLocked()
	if len(previous) == 0 {
		_ = os.Remove(m.ConfigPath)
		return
	}
	if err := atomicWrite(m.ConfigPath, previous, 0o600); err != nil {
		m.lastError = "restore previous relay configuration: " + err.Error()
		return
	}
	if err := m.startAndVerifyLocked(); err != nil {
		m.lastError = "restore previous relay process: " + err.Error()
	}
}

func (m *RelayManager) Shutdown() {
	m.operationMu.Lock()
	defer m.operationMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopLocked()
}

func (m *RelayManager) stopLocked() {
	m.cancelTargetProbesLocked()
	if m.cmd == nil || m.cmd.Process == nil {
		return
	}
	done := m.done
	_ = m.cmd.Process.Kill()
	if done != nil {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}
	m.cmd = nil
	m.done = nil
}

func normalizeRelayRules(rules []RelayRule) []string {
	nodes := make([]string, 0, len(rules)*2)
	seenListeners := map[string]bool{}
	for _, rule := range rules {
		listenHost := firstString(rule.ListenHost, rule.ListenHostAlt, rule.LocalHost, rule.LocalHostAlt, "0.0.0.0")
		targetHost := firstString(rule.TargetHost, rule.TargetHostAlt, rule.RemoteHost, rule.RemoteHostAlt, rule.Host)
		listenPort := firstPort(rule.ListenPort, rule.ListenPortAlt, rule.LocalPort, rule.LocalPortAlt)
		targetPort := firstPort(rule.TargetPort, rule.TargetPortAlt, rule.RemotePort, rule.RemotePortAlt, rule.Port)
		if listenPort < 1 || listenPort > 65535 || targetPort < 1 || targetPort > 65535 || targetHost == "" {
			continue
		}
		protocols := []string(rule.Protocols)
		if len(protocols) == 0 {
			protocols = []string{firstString(rule.Protocol, rule.Type)}
		}
		for _, protocol := range normalizeProtocols(protocols) {
			listenAddress := ":" + strconv.Itoa(listenPort)
			if listenHost != "" && listenHost != "0.0.0.0" && listenHost != "::" {
				listenAddress = net.JoinHostPort(strings.Trim(listenHost, "[]"), strconv.Itoa(listenPort))
			}
			listener := protocol + "://" + listenAddress
			node := listener + "/" + net.JoinHostPort(strings.Trim(targetHost, "[]"), strconv.Itoa(targetPort))
			if !seenListeners[listener] {
				seenListeners[listener] = true
				nodes = append(nodes, node)
			}
		}
	}
	return nodes
}

func relayListenersFromNodes(nodes []string) []relayListener {
	listeners := make([]relayListener, 0, len(nodes))
	seen := map[string]bool{}
	for _, node := range nodes {
		parts := strings.SplitN(node, "://", 2)
		if len(parts) != 2 {
			continue
		}
		protocol := strings.ToLower(strings.TrimSpace(parts[0]))
		endpoints := strings.SplitN(parts[1], "/", 2)
		endpoint := endpoints[0]
		host, portText, err := net.SplitHostPort(endpoint)
		if err != nil {
			continue
		}
		port, err := strconv.Atoi(portText)
		if err != nil || port < 1 || port > 65535 {
			continue
		}
		host = strings.Trim(strings.TrimSpace(host), "[]")
		if host == "" {
			host = "0.0.0.0"
		}
		targetHost := ""
		targetPort := 0
		if len(endpoints) == 2 {
			parsedHost, parsedPort, targetErr := net.SplitHostPort(endpoints[1])
			if targetErr == nil {
				targetHost = strings.Trim(strings.TrimSpace(parsedHost), "[]")
				targetPort, _ = strconv.Atoi(parsedPort)
			}
		}
		key := protocol + "://" + host + ":" + strconv.Itoa(port)
		if seen[key] {
			continue
		}
		seen[key] = true
		listeners = append(listeners, relayListener{
			Protocol:   protocol,
			ListenHost: host,
			ListenPort: port,
			TargetHost: targetHost,
			TargetPort: targetPort,
		})
	}
	return listeners
}

func processListeningSockets(pid int, listeners []relayListener) map[string]bool {
	result := map[string]bool{}
	if runtime.GOOS != "linux" || pid <= 0 {
		return result
	}
	expected := map[string]bool{}
	protocols := map[string]bool{}
	for _, listener := range listeners {
		expected[listener.Protocol+":"+strconv.Itoa(listener.ListenPort)] = true
		protocols[listener.Protocol] = true
	}
	if len(expected) == 0 {
		return result
	}
	candidates := map[string]string{}
	unresolved := map[string]bool{}
	for _, table := range []struct {
		path     string
		protocol string
		state    string
	}{
		{"/proc/net/tcp", "tcp", "0A"},
		{"/proc/net/tcp6", "tcp", "0A"},
		{"/proc/net/udp", "udp", "07"},
		{"/proc/net/udp6", "udp", "07"},
	} {
		if !protocols[table.protocol] {
			continue
		}
		file, err := os.Open(table.path)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			port, inode, ok := parseListeningSocketLine(scanner.Bytes(), table.state)
			if !ok {
				continue
			}
			key := table.protocol + ":" + strconv.Itoa(int(port))
			if expected[key] {
				// Copy the inode before the scanner reuses its buffer.
				candidates[string(inode)] = key
				unresolved[key] = true
			}
		}
		_ = file.Close()
	}
	if len(candidates) == 0 {
		return result
	}
	directory := filepath.Join("/proc", strconv.Itoa(pid), "fd")
	file, err := os.Open(directory)
	if err != nil {
		return result
	}
	defer file.Close()
	for {
		entries, readErr := file.ReadDir(128)
		for _, entry := range entries {
			target, err := os.Readlink(filepath.Join(directory, entry.Name()))
			if err != nil || !strings.HasPrefix(target, "socket:[") || !strings.HasSuffix(target, "]") {
				continue
			}
			inode := strings.TrimSuffix(strings.TrimPrefix(target, "socket:["), "]")
			if key, ok := candidates[inode]; ok {
				result[key] = true
				delete(unresolved, key)
				if len(unresolved) == 0 {
					return result
				}
			}
		}
		if readErr != nil {
			return result
		}
	}
}

func parseListeningSocketLine(line []byte, state string) (uint16, []byte, bool) {
	var address, inode []byte
	position := 0
	for column := 0; column < 10; column++ {
		for position < len(line) && (line[position] == ' ' || line[position] == '\t') {
			position++
		}
		start := position
		for position < len(line) && line[position] != ' ' && line[position] != '\t' {
			position++
		}
		if start == position {
			return 0, nil, false
		}
		field := line[start:position]
		switch column {
		case 1:
			address = field
		case 3:
			if !strings.EqualFold(string(field), state) {
				return 0, nil, false
			}
		case 9:
			inode = field
		}
	}
	separator := bytes.LastIndexByte(address, ':')
	if separator < 0 {
		return 0, nil, false
	}
	port, err := strconv.ParseUint(string(address[separator+1:]), 16, 16)
	if err != nil {
		return 0, nil, false
	}
	return uint16(port), inode, true
}

func normalizeProtocols(values []string) []string {
	result := make([]string, 0, 2)
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		items := []string{value}
		if value == "all" || value == "both" || value == "tcpudp" {
			items = []string{"tcp", "udp"}
		}
		for _, item := range items {
			if (item == "tcp" || item == "udp") && !seen[item] {
				seen[item] = true
				result = append(result, item)
			}
		}
	}
	return result
}

func firstString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstPort(values ...flexiblePort) int {
	for _, value := range values {
		if value > 0 {
			return int(value)
		}
	}
	return 0
}
