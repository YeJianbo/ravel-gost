package agent

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"golang.org/x/mod/semver"
)

type AutoUpdateConfig struct {
	Enabled         bool   `json:"enabled"`
	IntervalSeconds int    `json:"interval_seconds"`
	Repo            string `json:"repo"`
	RequestID       string `json:"request_id"`
	TargetVersion   string `json:"target_version"`
	RequestedAt     int64  `json:"requested_at"`
}

type githubRelease struct {
	TagName string        `json:"tag_name"`
	Assets  []githubAsset `json:"assets"`
}

type githubAsset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
	Digest             string `json:"digest"`
}

type ravelReleaseTarget struct {
	AssetName  string
	BinaryName string
}

type PendingRavelUpdate struct {
	PreviousVersion string `json:"previous_version"`
	TargetVersion   string `json:"target_version"`
	RequestID       string `json:"request_id,omitempty"`
	CreatedAt       int64  `json:"created_at"`
}

const trustedRavelUpdateRepo = "YeJianbo/ravel-gost"

func normalizeAutoUpdateConfig(config AutoUpdateConfig) AutoUpdateConfig {
	if config.IntervalSeconds < 3600 {
		config.IntervalSeconds = 86400
	}
	config.Repo = strings.TrimSpace(config.Repo)
	if config.Repo == "" || strings.EqualFold(config.Repo, trustedRavelUpdateRepo) {
		config.Repo = trustedRavelUpdateRepo
	}
	return config
}

func AutoUpdateInterval(config AutoUpdateConfig) time.Duration {
	return time.Duration(normalizeAutoUpdateConfig(config).IntervalSeconds) * time.Second
}

func CheckAndInstallRavelUpdate(ctx context.Context, config AutoUpdateConfig, currentVersion, binaryPath string) (string, bool, error) {
	config = normalizeAutoUpdateConfig(config)
	if !config.Enabled {
		return "", false, nil
	}
	currentVersion = strings.TrimSpace(currentVersion)
	if !semver.IsValid(currentVersion) {
		return "", false, fmt.Errorf("current Ravel version %q is not a release version", currentVersion)
	}

	release, err := fetchGitHubRelease(ctx, config.Repo, "")
	if err != nil {
		return "", false, err
	}
	if !semver.IsValid(release.TagName) {
		return "", false, fmt.Errorf("latest release has invalid version %q", release.TagName)
	}
	if semver.Compare(release.TagName, currentVersion) <= 0 {
		return release.TagName, false, nil
	}
	if err := installRavelRelease(ctx, release, binaryPath, currentVersion, ""); err != nil {
		return release.TagName, false, err
	}
	return release.TagName, true, nil
}

var githubAPIBaseURL = "https://api.github.com"

func CheckAndInstallRequestedRavelUpdate(ctx context.Context, config AutoUpdateConfig, currentVersion, binaryPath string) (string, bool, error) {
	config = normalizeAutoUpdateConfig(config)
	if strings.TrimSpace(config.RequestID) == "" {
		return "", false, nil
	}

	currentVersion = strings.TrimSpace(currentVersion)
	targetVersion := strings.TrimSpace(config.TargetVersion)
	if targetVersion != "" {
		if err := validateRequestedRavelVersion(currentVersion, targetVersion); err != nil {
			return "", false, err
		}
	}

	release, err := fetchGitHubRelease(ctx, config.Repo, targetVersion)
	if err != nil {
		return "", false, err
	}
	if err := validateRequestedRavelVersion(currentVersion, release.TagName); err != nil {
		return "", false, err
	}
	if targetVersion != "" && release.TagName != targetVersion {
		return "", false, fmt.Errorf("release does not match requested version")
	}
	if strings.TrimSpace(currentVersion) == release.TagName {
		return release.TagName, false, nil
	}
	if err := installRavelRelease(ctx, release, binaryPath, currentVersion, config.RequestID); err != nil {
		return release.TagName, false, err
	}
	return release.TagName, true, nil
}

func validateRequestedRavelVersion(currentVersion, targetVersion string) error {
	if !semver.IsValid(targetVersion) {
		return fmt.Errorf("requested release has an invalid version")
	}
	if semver.IsValid(currentVersion) && semver.Compare(targetVersion, currentVersion) < 0 {
		return fmt.Errorf("remote Ravel downgrade is not allowed")
	}
	return nil
}

func fetchGitHubRelease(ctx context.Context, repo, targetVersion string) (githubRelease, error) {
	var release githubRelease
	// Update code runs with machine privileges; the panel cannot select its trust source.
	if repo != trustedRavelUpdateRepo {
		return release, fmt.Errorf("release repository is not locally trusted")
	}
	endpoint := strings.TrimRight(githubAPIBaseURL, "/") + "/repos/" + repo + "/releases/latest"
	if targetVersion != "" {
		endpoint = strings.TrimRight(githubAPIBaseURL, "/") + "/repos/" + repo + "/releases/tags/" + url.PathEscape(targetVersion)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return release, err
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("User-Agent", "ravel-gost-updater")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return release, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return release, fmt.Errorf("GitHub release API returned %s", response.Status)
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(&release); err != nil {
		return release, err
	}
	return release, nil
}

func installRavelRelease(ctx context.Context, release githubRelease, binaryPath, currentVersion, requestID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	pending, err := LoadPendingRavelUpdate(binaryPath)
	if err == nil {
		if pending.TargetVersion == release.TagName {
			return nil
		}
		return fmt.Errorf("Ravel update to %s is still pending restart", pending.TargetVersion)
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("read pending Ravel update state: %w", err)
	}

	target, err := currentRavelReleaseTarget()
	if err != nil {
		return err
	}
	var selected githubAsset
	for _, asset := range release.Assets {
		if asset.Name == target.AssetName {
			selected = asset
			break
		}
	}
	if selected.BrowserDownloadURL == "" {
		return fmt.Errorf("release asset %s was not found", target.AssetName)
	}

	digest, err := resolveReleaseAssetDigest(ctx, release, selected)
	if err != nil {
		return err
	}
	archive, err := downloadReleaseArchive(ctx, selected.BrowserDownloadURL, filepath.Dir(binaryPath), digest)
	if err != nil {
		return err
	}
	defer os.Remove(archive.Name())
	defer archive.Close()
	archiveInfo, err := archive.Stat()
	if err != nil {
		return err
	}
	if err := replaceRavelBinaryReaderAtContext(ctx, binaryPath, archive, archiveInfo.Size(), target.BinaryName, func(path string) error {
		return validateRavelBinary(ctx, path, release.TagName)
	}); err != nil {
		return err
	}
	if err := writePendingRavelUpdate(binaryPath, PendingRavelUpdate{
		PreviousVersion: currentVersion,
		TargetVersion:   release.TagName,
		RequestID:       requestID,
		CreatedAt:       time.Now().Unix(),
	}); err != nil {
		_ = RestorePreviousRavelBinary(binaryPath)
		return fmt.Errorf("write pending update state: %w", err)
	}
	return nil
}

var runtimeGOOS = runtime.GOOS
var runtimeGOARCH = runtime.GOARCH
var readCurrentBuildSetting = currentBuildSetting

func currentRavelReleaseTarget() (ravelReleaseTarget, error) {
	arch := map[string]string{
		"386":      "32",
		"amd64":    "64",
		"arm64":    "arm64-v8a",
		"mips":     "mips32",
		"mipsle":   "mips32le",
		"mips64":   "mips64",
		"mips64le": "mips64le",
		"ppc64":    "ppc64",
		"ppc64le":  "ppc64le",
		"riscv64":  "riscv64",
		"s390x":    "s390x",
	}[runtimeGOARCH]
	binaryName := "ravel"
	if runtimeGOARCH == "arm" {
		goarm := readCurrentBuildSetting("GOARM")
		switch {
		case strings.HasPrefix(goarm, "5"):
			arch = "arm32-v5"
		case strings.HasPrefix(goarm, "6"):
			arch = "arm32-v6"
		default:
			arch = "arm32-v7a"
		}
	}
	if (runtimeGOARCH == "mips" || runtimeGOARCH == "mipsle") && readCurrentBuildSetting("GOMIPS") == "softfloat" {
		binaryName = "ravel_softfloat"
	}
	if arch == "" || runtimeGOOS != "linux" {
		return ravelReleaseTarget{}, fmt.Errorf("Ravel auto update does not support %s/%s", runtimeGOOS, runtimeGOARCH)
	}
	return ravelReleaseTarget{AssetName: "ravel-gost-linux-" + arch + ".zip", BinaryName: binaryName}, nil
}

func ravelReleaseAssetName() (string, error) {
	target, err := currentRavelReleaseTarget()
	return target.AssetName, err
}

func currentBuildSetting(name string) string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, setting := range info.Settings {
		if setting.Key == name {
			return setting.Value
		}
	}
	return ""
}

func downloadUpdateChecksum(ctx context.Context, url string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("User-Agent", "ravel-gost-updater")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download release asset returned %s", response.Status)
	}
	const maxChecksumBytes = 64 << 10
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxChecksumBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > maxChecksumBytes {
		return nil, fmt.Errorf("release checksum file exceeds %d bytes", maxChecksumBytes)
	}
	return raw, nil
}

func downloadReleaseArchive(ctx context.Context, url, directory, digest string) (*os.File, error) {
	algorithm, expected, ok := strings.Cut(strings.TrimSpace(digest), ":")
	if !ok || !strings.EqualFold(algorithm, "sha256") {
		return nil, fmt.Errorf("unsupported release digest %q", digest)
	}
	expectedBytes, err := hex.DecodeString(strings.TrimSpace(expected))
	if err != nil || len(expectedBytes) != sha256.Size {
		return nil, fmt.Errorf("invalid SHA-256 release digest %q", digest)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("User-Agent", "ravel-gost-updater")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download release asset returned %s", response.Status)
	}
	archive, err := os.CreateTemp(directory, ".ravel-archive-*")
	if err != nil {
		return nil, err
	}
	valid := false
	defer func() {
		if !valid {
			archive.Close()
			os.Remove(archive.Name())
		}
	}()
	var checksum hash.Hash = sha256.New()
	const maxArchiveSize = 128 << 20
	size, err := io.Copy(io.MultiWriter(archive, checksum), io.LimitReader(response.Body, maxArchiveSize+1))
	if err != nil {
		return nil, err
	}
	if size > maxArchiveSize {
		return nil, fmt.Errorf("release archive exceeds %d bytes", maxArchiveSize)
	}
	if !bytes.Equal(checksum.Sum(nil), expectedBytes) {
		return nil, fmt.Errorf("release asset checksum mismatch: expected %s, got %s", expected, hex.EncodeToString(checksum.Sum(nil)))
	}
	if err := archive.Sync(); err != nil {
		return nil, err
	}
	valid = true
	return archive, nil
}

func resolveReleaseAssetDigest(ctx context.Context, release githubRelease, asset githubAsset) (string, error) {
	if strings.TrimSpace(asset.Digest) != "" {
		return asset.Digest, nil
	}
	for _, candidate := range release.Assets {
		if candidate.Name != asset.Name+".dgst" || candidate.BrowserDownloadURL == "" {
			continue
		}
		raw, err := downloadUpdateChecksum(ctx, candidate.BrowserDownloadURL)
		if err != nil {
			return "", err
		}
		for _, line := range strings.Split(string(raw), "\n") {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 && strings.EqualFold(strings.TrimSpace(parts[0]), "SHA2-256") {
				return "sha256:" + strings.TrimSpace(parts[1]), nil
			}
		}
	}
	return "", fmt.Errorf("release asset %s does not provide a SHA-256 digest", asset.Name)
}

func verifyReleaseAssetDigest(data []byte, digest string) error {
	algorithm, expected, ok := strings.Cut(strings.TrimSpace(digest), ":")
	if !ok || !strings.EqualFold(algorithm, "sha256") {
		return fmt.Errorf("unsupported release digest %q", digest)
	}
	expectedBytes, err := hex.DecodeString(strings.TrimSpace(expected))
	if err != nil || len(expectedBytes) != sha256.Size {
		return fmt.Errorf("invalid SHA-256 release digest %q", digest)
	}
	actual := sha256.Sum256(data)
	if !bytes.Equal(actual[:], expectedBytes) {
		return fmt.Errorf("release asset checksum mismatch: expected %s, got %s", expected, hex.EncodeToString(actual[:]))
	}
	return nil
}

func replaceRavelBinary(binaryPath string, archive []byte) error {
	return replaceRavelBinaryEntry(binaryPath, archive, "ravel", nil)
}

func replaceRavelBinaryEntry(binaryPath string, archive []byte, binaryName string, validator func(string) error) error {
	return replaceRavelBinaryReaderAt(binaryPath, bytes.NewReader(archive), int64(len(archive)), binaryName, validator)
}

func replaceRavelBinaryReaderAt(binaryPath string, archive io.ReaderAt, size int64, binaryName string, validator func(string) error) error {
	return replaceRavelBinaryReaderAtContext(context.Background(), binaryPath, archive, size, binaryName, validator)
}

type updateContextReader struct {
	ctx    context.Context
	source io.Reader
}

func (r updateContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.source.Read(p)
}

func replaceRavelBinaryReaderAtContext(ctx context.Context, binaryPath string, archive io.ReaderAt, size int64, binaryName string, validator func(string) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	reader, err := zip.NewReader(archive, size)
	if err != nil {
		return fmt.Errorf("open release archive: %w", err)
	}
	var binary *zip.File
	for _, entry := range reader.File {
		if err := ctx.Err(); err != nil {
			return err
		}
		if filepath.Base(entry.Name) == binaryName && !entry.FileInfo().IsDir() {
			binary = entry
			break
		}
	}
	if binary == nil {
		return fmt.Errorf("release archive does not contain %s", binaryName)
	}
	const maxBinaryBytes = 256 << 20
	if binary.UncompressedSize64 > maxBinaryBytes {
		return fmt.Errorf("release binary exceeds %d bytes", maxBinaryBytes)
	}

	source, err := binary.Open()
	if err != nil {
		return err
	}
	defer source.Close()
	temporary, err := os.CreateTemp(filepath.Dir(binaryPath), ".ravel-update-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	extractedBytes, err := io.Copy(temporary, io.LimitReader(updateContextReader{ctx: ctx, source: source}, maxBinaryBytes+1))
	if err != nil {
		temporary.Close()
		return err
	}
	if extractedBytes > maxBinaryBytes {
		temporary.Close()
		return fmt.Errorf("release binary exceeds %d bytes", maxBinaryBytes)
	}
	if err := ctx.Err(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Chmod(0o755); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if validator != nil {
		if err := validator(temporaryPath); err != nil {
			return fmt.Errorf("validate new Ravel binary: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Finish activation or rollback once the rename pair begins.
	backupPath := binaryPath + ".previous"
	_ = os.Remove(backupPath)
	if err := os.Rename(binaryPath, backupPath); err != nil {
		return fmt.Errorf("backup current Ravel binary: %w", err)
	}
	if err := os.Rename(temporaryPath, binaryPath); err != nil {
		_ = os.Rename(backupPath, binaryPath)
		return fmt.Errorf("activate Ravel update: %w", err)
	}
	return nil
}

func validateRavelBinary(ctx context.Context, path, expectedVersion string) error {
	output, _, err := runRuntimeCommandContext(ctx, 15*time.Second, path, "version")
	if err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(output))
	}
	outputText := strings.TrimSpace(output)
	if !strings.Contains(strings.ToLower(outputText), "gost-only") || !ravelVersionOutputMatches(outputText, expectedVersion) {
		return fmt.Errorf("unexpected version output %q, expected %s", outputText, expectedVersion)
	}
	return nil
}

func ravelVersionOutputMatches(output, expectedVersion string) bool {
	expectedVersion = strings.TrimSpace(expectedVersion)
	if expectedVersion == "" {
		return false
	}
	for _, field := range strings.Fields(output) {
		if strings.Trim(field, "()[]{}") == expectedVersion {
			return true
		}
	}
	return false
}

func pendingRavelUpdatePath(binaryPath string) string {
	return binaryPath + ".update-pending.json"
}

func writePendingRavelUpdate(binaryPath string, state PendingRavelUpdate) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return atomicWrite(pendingRavelUpdatePath(binaryPath), append(raw, '\n'), 0o600)
}

func LoadPendingRavelUpdate(binaryPath string) (PendingRavelUpdate, error) {
	var state PendingRavelUpdate
	raw, err := readBoundedAgentFile(pendingRavelUpdatePath(binaryPath), maxAgentStateFileBytes)
	if err != nil {
		return state, err
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return state, err
	}
	return state, nil
}

func CompletePendingRavelUpdate(binaryPath string) error {
	if err := os.Remove(pendingRavelUpdatePath(binaryPath)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(binaryPath + ".previous"); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func RestorePreviousRavelBinary(binaryPath string) error {
	backupPath := binaryPath + ".previous"
	if _, err := os.Stat(backupPath); err != nil {
		return err
	}
	failedPath := binaryPath + ".failed"
	_ = os.Remove(failedPath)
	if err := os.Rename(binaryPath, failedPath); err != nil {
		return err
	}
	if err := os.Rename(backupPath, binaryPath); err != nil {
		_ = os.Rename(failedPath, binaryPath)
		return err
	}
	_ = os.Remove(failedPath)
	_ = os.Remove(pendingRavelUpdatePath(binaryPath))
	return nil
}
