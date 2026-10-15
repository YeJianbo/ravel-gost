package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const networkQualityPath = "/api/v1/server/machine/networkQuality"

var (
	packetSummaryPattern = regexp.MustCompile(`(?m)(\d+)\s+packets transmitted,\s+(\d+)\s+(?:packets )?received`)
	rttPattern           = regexp.MustCompile(`(?m)=\s*([0-9.]+)/([0-9.]+)/([0-9.]+)(?:/[0-9.]+)?\s*ms`)
)

type NetworkQualityTarget struct {
	Key       string `json:"key"`
	Name      string `json:"name"`
	Host      string `json:"host"`
	ProbeType string `json:"probe_type"`
	Port      int    `json:"port"`
	IPVersion string `json:"ip_version"`
	Enabled   *bool  `json:"enabled"`
}

type NetworkQualityConfig struct {
	Enabled         bool                   `json:"enabled"`
	IntervalSeconds int                    `json:"interval_seconds"`
	PacketCount     int                    `json:"packet_count"`
	TimeoutSeconds  int                    `json:"timeout_seconds"`
	Targets         []NetworkQualityTarget `json:"targets"`
}

type NetworkQualitySample struct {
	Key        string   `json:"key"`
	Sent       int      `json:"sent"`
	Received   int      `json:"received"`
	PacketLoss float64  `json:"packet_loss"`
	LatencyMin *float64 `json:"latency_min"`
	LatencyAvg *float64 `json:"latency_avg"`
	LatencyMax *float64 `json:"latency_max"`
}

func normalizeNetworkQualityConfig(config NetworkQualityConfig) NetworkQualityConfig {
	if config.IntervalSeconds < 60 || config.IntervalSeconds > 3600 {
		config.IntervalSeconds = 300
	}
	if config.PacketCount < 1 || config.PacketCount > 20 {
		config.PacketCount = 5
	}
	if config.TimeoutSeconds < 1 || config.TimeoutSeconds > 10 {
		config.TimeoutSeconds = 2
	}
	if len(config.Targets) < 1 || len(config.Targets) > 8 || len(enabledNetworkQualityTargets(config.Targets)) < 1 {
		config.Enabled = false
	}
	for index := range config.Targets {
		if !validNetworkHost(config.Targets[index].Host) {
			config.Enabled = false
		}
		if config.Targets[index].ProbeType != "tcp" {
			config.Targets[index].ProbeType = "icmp"
		}
		if config.Targets[index].Port < 1 || config.Targets[index].Port > 65535 {
			config.Targets[index].Port = 80
		}
		if config.Targets[index].IPVersion != "4" && config.Targets[index].IPVersion != "6" {
			config.Targets[index].IPVersion = "auto"
		}
	}
	return config
}

func (c *Controller) ProbeAndPushNetworkQuality() error {
	return c.ProbeAndPushNetworkQualityContext(context.Background())
}

func (c *Controller) ProbeAndPushNetworkQualityContext(ctx context.Context) error {
	config := c.NetworkQualityConfig()
	if !config.Enabled {
		return nil
	}
	samples, err := collectNetworkQualityContext(ctx, config)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if NetworkQualityFingerprint(config) != NetworkQualityFingerprint(c.NetworkQualityConfig()) {
		return nil
	}
	var response map[string]any
	return c.Client.PostContext(ctx, networkQualityPath, map[string]any{"samples": samples}, &response)
}

func collectNetworkQualityContext(ctx context.Context, config NetworkQualityConfig) ([]NetworkQualitySample, error) {
	targets := enabledNetworkQualityTargets(config.Targets)
	samples := make([]NetworkQualitySample, len(targets))
	probeErrors := make([]error, len(targets))
	jobs := make(chan int)
	var group sync.WaitGroup
	// Limit simultaneous ping processes and DNS/dial work on small probe hosts.
	for range min(2, len(targets)) {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := range jobs {
				if ctx.Err() != nil {
					return
				}
				samples[index], probeErrors[index] = probeNetworkQualityTargetContext(ctx, targets[index], config.PacketCount, config.TimeoutSeconds)
			}
		}()
	}
queue:
	for index := range targets {
		select {
		case jobs <- index:
		case <-ctx.Done():
			break queue
		}
	}
	close(jobs)
	group.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// The panel accepts complete rounds only; never substitute failed probes with loss.
	if err := errors.Join(probeErrors...); err != nil {
		return nil, err
	}
	return samples, nil
}

func probeNetworkQualityTargetContext(ctx context.Context, target NetworkQualityTarget, count, timeoutSeconds int) (NetworkQualitySample, error) {
	if target.ProbeType == "tcp" {
		sample := tcpNetworkQualityTargetContext(ctx, target, count, timeoutSeconds)
		return sample, ctx.Err()
	}
	return pingNetworkQualityTargetContext(ctx, target, count, timeoutSeconds)
}

func tcpNetworkQualityTarget(target NetworkQualityTarget, count, timeoutSeconds int) NetworkQualitySample {
	return tcpNetworkQualityTargetContext(context.Background(), target, count, timeoutSeconds)
}

func tcpNetworkQualityTargetContext(ctx context.Context, target NetworkQualityTarget, count, timeoutSeconds int) NetworkQualitySample {
	sample := NetworkQualitySample{Key: target.Key, Sent: count, PacketLoss: 100}
	network := "tcp"
	if target.IPVersion == "4" || target.IPVersion == "6" {
		network += target.IPVersion
	}
	address := net.JoinHostPort(target.Host, strconv.Itoa(target.Port))
	latencies := make([]float64, 0, count)
	dialer := net.Dialer{Timeout: time.Duration(timeoutSeconds) * time.Second}
	for range count {
		if ctx.Err() != nil {
			break
		}
		startedAt := time.Now()
		connection, err := dialer.DialContext(ctx, network, address)
		if err != nil {
			continue
		}
		latencies = append(latencies, float64(time.Since(startedAt).Microseconds())/1000)
		_ = connection.Close()
	}
	sample.Received = len(latencies)
	if count > 0 {
		sample.PacketLoss = float64(count-sample.Received) * 100 / float64(count)
	}
	if len(latencies) > 0 {
		minimum, maximum, total := latencies[0], latencies[0], 0.0
		for _, latency := range latencies {
			minimum = min(minimum, latency)
			maximum = max(maximum, latency)
			total += latency
		}
		average := total / float64(len(latencies))
		sample.LatencyMin = &minimum
		sample.LatencyAvg = &average
		sample.LatencyMax = &maximum
	}
	return sample
}

func enabledNetworkQualityTargets(targets []NetworkQualityTarget) []NetworkQualityTarget {
	enabled := make([]NetworkQualityTarget, 0, len(targets))
	for _, target := range targets {
		if target.Enabled == nil || *target.Enabled {
			enabled = append(enabled, target)
		}
	}
	return enabled
}

func pingNetworkQualityTargetContext(ctx context.Context, target NetworkQualityTarget, count, timeoutSeconds int) (NetworkQualitySample, error) {
	args := networkQualityPingArgs(target, count, timeoutSeconds, runtime.GOOS)
	// Allow each packet's timeout and interval, plus startup/DNS overhead.
	timeout := time.Duration(count*(timeoutSeconds+1)+5) * time.Second
	output, exitCode, err := runRuntimeCommandContext(ctx, timeout, "ping", args...)
	if err != nil {
		var exitError *exec.ExitError
		// ping exits with 1 for real packet loss; command/setup errors are not measurements.
		if exitCode != 1 || !errors.As(err, &exitError) {
			return NetworkQualitySample{}, fmt.Errorf("network quality target %q: ping failed: %w", target.Key, err)
		}
	}
	sample := parsePingOutput(target.Key, count, output)
	if sample.Sent != count {
		return NetworkQualitySample{}, fmt.Errorf("network quality target %q: ping returned no complete packet summary", target.Key)
	}
	return sample, nil
}

func networkQualityPingArgs(target NetworkQualityTarget, count, timeoutSeconds int, goos string) []string {
	args := []string{"-n"}
	if target.IPVersion == "4" || target.IPVersion == "6" {
		args = append(args, "-"+target.IPVersion)
	}
	args = append(args, "-c", strconv.Itoa(count), "-W", strconv.Itoa(timeoutSeconds), target.Host)
	if goos == "windows" {
		args = []string{}
		if target.IPVersion == "4" || target.IPVersion == "6" {
			args = append(args, "-"+target.IPVersion)
		}
		args = append(args, "-n", strconv.Itoa(count), "-w", strconv.Itoa(timeoutSeconds*1000), target.Host)
	}
	return args
}

func parsePingOutput(key string, requested int, output string) NetworkQualitySample {
	sample := NetworkQualitySample{Key: key}
	match := packetSummaryPattern.FindStringSubmatch(output)
	if len(match) != 3 {
		return sample
	}
	sent, sentErr := strconv.Atoi(match[1])
	received, receivedErr := strconv.Atoi(match[2])
	if sentErr != nil || receivedErr != nil || sent != requested || sent < 1 || received > sent {
		return sample
	}
	sample.Sent, sample.Received = sent, received
	sample.PacketLoss = float64(sent-received) * 100 / float64(sent)
	if match := rttPattern.FindStringSubmatch(output); received > 0 && len(match) == 4 {
		minimum, minErr := strconv.ParseFloat(match[1], 64)
		average, avgErr := strconv.ParseFloat(match[2], 64)
		maximum, maxErr := strconv.ParseFloat(match[3], 64)
		if minErr == nil && avgErr == nil && maxErr == nil && minimum <= average && average <= maximum {
			sample.LatencyMin = &minimum
			sample.LatencyAvg = &average
			sample.LatencyMax = &maximum
		}
	}
	return sample
}

func NetworkQualityFingerprint(config NetworkQualityConfig) string {
	parts := []string{
		strconv.FormatBool(config.Enabled),
		strconv.Itoa(config.IntervalSeconds),
		strconv.Itoa(config.PacketCount),
		strconv.Itoa(config.TimeoutSeconds),
	}
	for _, target := range config.Targets {
		parts = append(parts, target.Key, target.Host, target.ProbeType, strconv.Itoa(target.Port), target.IPVersion, strconv.FormatBool(target.Enabled == nil || *target.Enabled))
	}
	return strings.Join(parts, "|")
}

func NetworkQualityInterval(config NetworkQualityConfig) time.Duration {
	return time.Duration(config.IntervalSeconds) * time.Second
}
