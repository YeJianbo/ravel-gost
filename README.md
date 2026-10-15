# Ravel GOST

独立的 Linux 转发与机器监控服务。仅包含 Ravel 管理程序和 GOST 转发程序，支持 TCP/UDP 端口转发、资源上报、网络质量监测和 Ravel 自动更新。

## 安装

支持 systemd 或 Alpine/OpenRC、Linux amd64 / arm64。需要 `bash`、`curl`、`jq`、`unzip`、`sha256sum`，以及系统自带的用户管理工具。Alpine 可以先执行 `apk add bash curl jq unzip`。

从 [Releases](https://github.com/YeJianbo/ravel-gost/releases) 下载对应架构的 ZIP 与 `.sha256` 文件，校验后解压。查看包内 `install.sh` 和服务文件后执行：

```bash
sha256sum -c ravel-gost-linux-64.zip.sha256
unzip ravel-gost-linux-64.zip -d ravel-gost
cd ravel-gost
sudo bash install.sh --panel https://panel.example.com --machine-id 1
```

安装时隐藏输入面板对应机器的 Token，不把 Token 放在命令参数或 Shell 历史里。自动部署可以用 `--token-stdin` 从标准输入读取。面板必须已提供 `/api/v1/server/machine/relayConfig` 接口。

首次安装不会覆盖已有目录、用户或服务。安装包和安装脚本应从本仓库可信发布流程获取，不执行面板即时生成的特权脚本。

## 运行与目录

```bash
systemctl status ravel-gost.service
journalctl -u ravel-gost.service -n 100 --no-pager
systemctl restart ravel-gost.service
```

- 服务名、运行用户：`ravel-gost`。
- 程序：`/usr/local/ravel-gost/ravel`、`/usr/local/ravel-gost/gost`。
- 凭据：`/etc/ravel-gost/state.json`，仅 root 和服务组可读。
- 转发缓存及状态：`/var/lib/ravel-gost`。
- 配置默认每 30 秒同步，状态每 120 秒上报。

OpenRC 使用 `rc-service ravel-gost status`、`rc-service ravel-gost restart` 管理服务，日志位于 `/var/log/ravel-gost.log`，需要定期轮转。ICMP 监测需要系统允许普通用户使用 `ping`，否则使用 TCP 探测，不为此提升服务权限。

程序不提供终端、远程 Shell、系统配置任务或防火墙管理。服务不是 root，最多接受 1024 条规则。systemd 服务仅保留绑定低端口的能力，文件写入限制在程序及数据目录，内存上限 512 MiB。OpenRC 仅提供普通用户和文件权限隔离，不等同于 systemd 沙箱，默认仅支持 1024 以上端口；需要进一步隔离时使用专用容器及宿主资源限制。转发仍能访问机器网络可达的地址，部署时应使用独立机器、网络分段和出站防火墙限制目标。主机管理员应避免给此用户授予额外权限。

## 更新

面板机器设置中的自动更新开关控制 Ravel 更新检查。程序仅接受本仓库的新正式版本，验证发布包 SHA-256 和程序版本后替换 Ravel，由服务管理器重启。面板不能改变更新源，不能要求降级。

GOST 固定为 2.11.2，CI 校验固定上游摘要后打包；Ravel 自动更新不替换 GOST。更换 GOST 需要主机管理员单独审核并维护。发布包摘要校验依赖 GitHub 发布源的可信性，不是独立签名验证。

停止并移除服务前可执行 `sudo systemctl disable --now ravel-gost.service`。在面板中删除对应机器记录，并由主机管理员检查后移除上述独立目录和用户；停止服务不会自动删除面板记录。

## 构建

```bash
go build -trimpath -ldflags '-s -w -X main.version=v0.1.0' -o ravel .
```

GitHub Actions 编译 amd64 / arm64 完整发布包。推送 `v*` 标签会创建 Release；不需要其他私有仓库或账户级构建凭据。源码采用 MPL-2.0；包内另附 GOST 的 MIT 许可证。
