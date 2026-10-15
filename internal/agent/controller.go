package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const configPath = "/api/v1/server/machine/relayConfig"
const statusPath = "/api/v1/server/machine/push"
const updateAckPath = "/api/v1/server/machine/updateAck"

type ConfigResponse struct {
	Revision       string               `json:"revision"`
	Relay          *RelayConfig         `json:"relay"`
	NetworkQuality NetworkQualityConfig `json:"network_quality"`
	AutoUpdate     AutoUpdateConfig     `json:"auto_update"`
}

type Controller struct {
	Client    *Client
	Relay     *RelayManager
	Directory string
	mu        sync.RWMutex
	quality   NetworkQualityConfig
	update    AutoUpdateConfig
	apply     map[string]any
}

func LoadState(path string) (State, error) {
	var state State
	raw, err := readBoundedAgentFile(path, 64<<10)
	if err != nil {
		return state, err
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return state, err
	}
	if state.SyncInterval < 5 || state.SyncInterval > 3600 {
		state.SyncInterval = 30
	}
	if state.StatusInterval < 10 || state.StatusInterval > 3600 {
		state.StatusInterval = 120
	}
	return state, nil
}

func (c *Controller) Sync(ctx context.Context) error {
	var config ConfigResponse
	if err := c.Client.GetContext(ctx, configPath, nil, &config); err != nil {
		return err
	}
	if config.Relay == nil || config.Revision == "" {
		return fmt.Errorf("panel omitted relay configuration")
	}
	_, err := c.Relay.SyncContext(ctx, *config.Relay)
	c.mu.Lock()
	c.apply = map[string]any{"config_desired_revision": config.Revision, "config_apply_at": time.Now().Unix()}
	if err != nil {
		c.apply["config_apply_status"] = "failed"
		c.apply["config_apply_error"] = err.Error()
	} else {
		c.apply["config_applied_revision"] = config.Revision
		c.apply["config_apply_status"] = "success"
		c.quality = normalizeNetworkQualityConfig(config.NetworkQuality)
		c.update = normalizeAutoUpdateConfig(config.AutoUpdate)
	}
	raw, _ := json.Marshal(c.apply)
	c.mu.Unlock()
	if saveErr := atomicWrite(filepath.Join(c.Directory, "apply.json"), raw, 0600); saveErr != nil {
		return saveErr
	}
	return err
}

func (c *Controller) NetworkQualityConfig() NetworkQualityConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	config := c.quality
	config.Targets = append([]NetworkQualityTarget(nil), config.Targets...)
	return config
}

func (c *Controller) PushStatus(ctx context.Context) error {
	status := CollectStatus()
	for key, value := range c.Client.PublicAddresses() {
		status[key] = value
	}
	for key, value := range c.Relay.Status() {
		status[key] = value
	}
	c.mu.RLock()
	for key, value := range c.apply {
		status[key] = value
	}
	c.mu.RUnlock()
	var reply map[string]any
	return c.Client.PostContext(ctx, statusPath, status, &reply)
}

func (c *Controller) Run(ctx context.Context, state State, version string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	StartNetworkRateSampler()
	SetRuntimeVersion(version)
	if err := c.Sync(ctx); err != nil {
		slog.Warn("initial relay sync failed; retaining cached rules", "error", err)
		if err := c.Relay.StartExistingContext(ctx); err != nil {
			return err
		}
	}
	var group sync.WaitGroup
	start := func(interval time.Duration, operation func(context.Context)) {
		group.Add(1)
		go func() {
			defer group.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					operation(ctx)
				}
			}
		}()
	}
	start(time.Duration(state.SyncInterval)*time.Second, func(ctx context.Context) {
		if err := c.Sync(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("relay sync failed", "error", err)
		}
	})
	start(time.Duration(state.StatusInterval)*time.Second, func(ctx context.Context) {
		if err := c.PushStatus(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("status report failed", "error", err)
		}
	})
	var qualityNext time.Time
	var updateNext time.Time
	start(5*time.Second, func(ctx context.Context) {
		quality := c.NetworkQualityConfig()
		if quality.Enabled && !time.Now().Before(qualityNext) {
			qualityNext = time.Now().Add(NetworkQualityInterval(quality))
			if err := c.ProbeAndPushNetworkQualityContext(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("network quality report failed", "error", err)
			}
		}
		c.mu.RLock()
		update := c.update
		healthy := c.apply["config_apply_status"] == "success"
		c.mu.RUnlock()
		binary, err := os.Executable()
		if err != nil || !healthy {
			return
		}
		pending, pendingErr := LoadPendingRavelUpdate(binary)
		if pendingErr == nil && pending.TargetVersion == version {
			if err := CompletePendingRavelUpdate(binary); err != nil {
				slog.Warn("confirm update failed", "error", err)
				return
			}
		}
		if update.Enabled && !time.Now().Before(updateNext) {
			updateNext = time.Now().Add(AutoUpdateInterval(update))
			updateCtx, stop := context.WithTimeout(ctx, 3*time.Minute)
			defer stop()
			_, changed, err := CheckAndInstallRavelUpdate(updateCtx, update, version, binary)
			if err != nil {
				slog.Warn("update check failed", "error", err)
			} else if changed {
				cancel()
			}
		}
	})
	if err := c.PushStatus(ctx); err != nil {
		slog.Warn("initial status report failed", "error", err)
	}
	<-ctx.Done()
	group.Wait()
	c.Relay.Shutdown()
	return nil
}
