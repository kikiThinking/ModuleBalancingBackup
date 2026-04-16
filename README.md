# ModuleBalancingbackupservice

`ModuleBalancingbackupservice` 是一个基于 Go、gRPC 和 MySQL 的模块备份服务，用于管理本地模块文件，并对外提供上传、下载、校验重载与存储容量判定能力。

服务启动后会自动加载本地模块目录、同步数据库记录、监听新增文件，并定时清理过期文件和无效记录。

## 核心能力

- gRPC 文件上传与下载
- 本地模块目录自动扫描与入库
- 新增文件自动监听、计算 CRC64 并更新数据库
- 过期模块自动清理
- 磁盘容量检查，决定是否允许继续存储
- 按业务分类输出运行日志

## 项目结构

```text
.
├── Modulebalancingbackup.go   # 程序入口
├── api/                       # gRPC 接口实现
├── db/                        # 数据模型
├── env/                       # 配置、目录监听、CRC64、系统工具
├── grpc/                      # proto 与生成代码
├── logmanager/                # 日志管理
└── conf/config.yaml           # 配置文件
```

## 运行要求

- Go 1.24.1
- MySQL
- Windows

当前实现依赖 Windows 文件相关 API，默认运行在 Windows 环境。

## 配置示例

配置文件位于 `conf/config.yaml`：

```yaml
Setting:
  Expiration: 180
  CheckExpiration: 24
  CheckUnwanted: 30
  Common: C:\Backserver\common
  ReserveSize: 42949672960

Database:
  Host: localhost
  Port: 3306
  Username: root
  Password: your-password

GRPC:
  Port: 9999
```

说明：

- `Common`：本地模块目录
- `Expiration`：文件过期天数
- `CheckExpiration` / `CheckUnwanted`：后台检查周期，单位分钟
- `ReserveSize`：磁盘预留空间，单位字节

## 快速启动

```powershell
go mod tidy
go build .
go run .
```

如果默认 Go 缓存目录不可写，可改用仓库内缓存：

```powershell
$env:GOCACHE="$PWD\.gocache"
go build ./...
```

## gRPC 接口

Proto 文件：`grpc/ModuleBalancing.proto`

- `Upload`：上传模块文件到备份节点
- `Push`：下载模块文件，支持断点续传
- `ModuleReload`：重新计算并更新文件校验信息
- `AllowStorage`：检查当前节点是否允许继续存储

## 说明

- 程序启动时会自动执行数据库迁移。
- 数据库名当前在代码中固定为 `modulebalancingbackup`。
- `grpc/*.pb.go` 为生成文件，接口变更应先修改 `.proto` 后再重新生成。
