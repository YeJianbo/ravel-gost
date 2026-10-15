//go:build linux

package agent

import "syscall"

func readDisk(status map[string]any) {
	var info syscall.Statfs_t
	if err := syscall.Statfs("/", &info); err != nil {
		return
	}
	total := info.Blocks * uint64(info.Bsize)
	free := info.Bfree * uint64(info.Bsize)
	used := uint64(0)
	if total >= free {
		used = total - free
	}
	status["disk_total"] = total
	status["disk_used"] = used
	status["disk"] = percent(used, total)
}
