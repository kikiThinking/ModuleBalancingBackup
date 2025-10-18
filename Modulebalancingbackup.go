/*
*

	@author: kiki
	@since: 2025/5/25
	@desc: //TODO

*
*/
package main

import (
	"ModuleBalancingbackupservice/api"
	"ModuleBalancingbackupservice/db"
	"ModuleBalancingbackupservice/env"
	rpc "ModuleBalancingbackupservice/grpc"
	"ModuleBalancingbackupservice/logmanager"
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"gopkg.in/yaml.v3"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	err                   error
	service               *grpc.Server
	dbcontrol             *gorm.DB
	servicesconfiguration *env.Configuration
	logmar                = logmanager.InitManager()
)

func init() {
	Programinformation()
	f, err := os.ReadFile(strings.Join([]string{"conf", "config.yaml"}, "/"))
	if err != nil {
		panic(err)
	}

	// 初始化日志
	logmar.RegisterBusiness(logmanager.LoggerConfig{
		BusinessName: "ClientDownload",
		LogDir:       fmt.Sprintf(strings.Join([]string{readrunpath(), "logs", "download"}, `\`)),
		MaxSize:      1,
		MaxBackups:   90,
		MinLevel:     logmanager.INFO,
	})

	// 初始化日志
	logmar.RegisterBusiness(logmanager.LoggerConfig{
		BusinessName: "Upload",
		LogDir:       fmt.Sprintf(strings.Join([]string{readrunpath(), "logs", "upload"}, `\`)),
		MaxSize:      1,
		MaxBackups:   90,
		MinLevel:     logmanager.INFO,
	})

	// 初始化日志
	logmar.RegisterBusiness(logmanager.LoggerConfig{
		BusinessName: "Monitornewmodule",
		LogDir:       fmt.Sprintf(strings.Join([]string{readrunpath(), "logs", "monitornewmodule"}, `\`)),
		MaxSize:      1,
		MaxBackups:   90,
		MinLevel:     logmanager.INFO,
	})

	// 初始化日志
	logmar.RegisterBusiness(logmanager.LoggerConfig{
		BusinessName: "LoadingLocalModule",
		LogDir:       fmt.Sprintf(strings.Join([]string{readrunpath(), "logs", "loadinglocalmodule"}, `\`)),
		MaxSize:      1,
		MaxBackups:   90,
		MinLevel:     logmanager.INFO,
	})

	// 初始化日志
	logmar.RegisterBusiness(logmanager.LoggerConfig{
		BusinessName: "Expiration",
		LogDir:       fmt.Sprintf(strings.Join([]string{readrunpath(), "logs", "expiration"}, `\`)),
		MaxSize:      1,
		MaxBackups:   90,
		MinLevel:     logmanager.INFO,
	})

	// 初始化日志
	logmar.RegisterBusiness(logmanager.LoggerConfig{
		BusinessName: "Unwanted",
		LogDir:       fmt.Sprintf(strings.Join([]string{readrunpath(), "logs", "unwanted"}, `\`)),
		MaxSize:      1,
		MaxBackups:   90,
		MinLevel:     logmanager.INFO,
	})

	servicesconfiguration = new(env.Configuration)
	if err = yaml.Unmarshal(f, servicesconfiguration); err != nil {
		panic(err)
	}

	if servicesconfiguration.Database.Port == "" || servicesconfiguration.Database.Host == "" || servicesconfiguration.Database.Username == "" || servicesconfiguration.Database.Password == "" {
		panic("Error: The database configuration information is incomplete, please check!")
	}

	// connect db
	if dbcontrol, err = gorm.Open(mysql.Open(fmt.Sprintf(`%s:%s@tcp(%s:%s)/modulebalancingbackup?charset=utf8mb4&parseTime=True&loc=Local`,
		servicesconfiguration.Database.Username,
		servicesconfiguration.Database.Password,
		servicesconfiguration.Database.Host,
		servicesconfiguration.Database.Port,
	)), &gorm.Config{
		Logger: logger.New(log.New(os.Stdout, "\r\n", log.Flags()), logger.Config{SlowThreshold: time.Second, LogLevel: logger.Warn, Colorful: true}),
	}); err != nil {
		panic(err)
	}

	// 设置连接池
	if dbobj, err := dbcontrol.DB(); err != nil {
		panic(err)
	} else {
		dbobj.SetMaxIdleConns(10)
		dbobj.SetMaxOpenConns(50)
		dbobj.SetConnMaxLifetime(time.Second * 30)
	}

	// 设置自动迁移模式
	if err = dbcontrol.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci").AutoMigrate(db.AutoMigrate()...); err != nil {
		panic(err)
	}
}

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", servicesconfiguration.GRPC.Port))
	if err != nil {
		panic(err)
	}

	// 启动时装载本地没有记录的Module
	if err = Loadmoduletodatabse(dbcontrol, logmar.GetLogger("LoadingLocalModule")); err != nil {
		panic(err)
	}

	// 监听本地Module的过期
	go expirationcheck(dbcontrol)
	// 实时监听Module目录, 当Module目录新增Module时
	go env.MonitornewmoduleBack(dbcontrol, logmar.GetLogger("Monitornewmodule"), servicesconfiguration.Setting.Expiration, servicesconfiguration.Setting.Common)

	// 本地文件不存在删除数据库记录
	go Removeunwantedrecord(dbcontrol)

	service = grpc.NewServer()
	rpc.RegisterModuleServer(service, &api.ModuleBalancing{Configuration: servicesconfiguration, Dbcontrol: dbcontrol, Logmar: logmar})
	reflection.Register(service)

	log.Println("Listening on :9999")
	if err = service.Serve(lis); err != nil {
		panic(err)
	}
}

// Loadmoduletodatabse 装载本地的Module文件, 每次程序启动时, 检查Module目录是否有新增文件
func Loadmoduletodatabse(ctx *gorm.DB, logwri *logmanager.BusinessLogger) error {
	var (
		filelist []os.DirEntry
	)

	if filelist, err = os.ReadDir(servicesconfiguration.Setting.Common); err != nil {
		return err
	}

	logwri.Info(fmt.Sprintf("Queue: %v", len(filelist)))
	for index, item := range filelist {
		if item.IsDir() {
			continue
		}

		logwri.Info(fmt.Sprintf("(%d) %s", index, item.Name()))

		var exist bool
		if err = ctx.Model(db.Module{}).Select(`COUNT(*) > 0`).Where(db.Module{Name: item.Name()}).Scan(&exist).
			Error; err != nil {
			return err
		}

		if !exist {
			logmar.GetLogger("Dumplocalmodules").Info(fmt.Sprintf("(%v)Discover new module(%s)", index, item.Name()))
			fmt.Printf("(%v)Discover new module(%s)", index, item.Name())

			var crc uint64
			var size int64
			if crc, size, err = env.CRC64(strings.Join([]string{servicesconfiguration.Setting.Common, item.Name()}, `\`), 128*1024*1024, 8); err != nil {
				logmar.GetLogger("Dumplocalmodules").Error(fmt.Sprintf("----> Failed(%s)", err.Error()))
				fmt.Printf("----> Failed(%s)\r\n", err.Error())
				continue
			}

			var module = db.Module{
				CRC64:      crc,
				Name:       item.Name(),
				Size:       size,
				Lastuse:    time.Now(),
				Expiration: time.Now().Add(time.Hour * 24 * time.Duration(servicesconfiguration.Setting.Expiration)),
			}

			var isexistrecord bool
			if err = dbcontrol.Unscoped().
				Model(db.Module{}).Select(`COUNT(*) > 0`).
				Where(db.Module{Name: module.Name}).
				Scan(&isexistrecord).Error; err != nil {
				return err
			}

			if isexistrecord {
				if err = dbcontrol.
					Unscoped().
					Model(db.Module{}).
					Where(db.Module{Name: module.Name}).
					Updates(map[string]interface{}{
						"crc64":      module.CRC64,
						"size":       module.Size,
						"lastuse":    module.Lastuse,
						"expiration": module.Expiration,
						"deleted_at": nil,
					}).Error; err != nil {
					logmar.GetLogger("Dumplocalmodules").Error(fmt.Sprintf("----> Failed(%s)", err.Error()))
					return err
				}
			} else {
				if err = dbcontrol.Model(db.Module{}).Create(&module).Error; err != nil {
					logmar.GetLogger("Dumplocalmodules").Error(fmt.Sprintf("----> Failed(%s)", err.Error()))
					return err
				}
			}

			//if err = ctx.Clauses(
			//	clause.OnConflict{
			//		Columns:   []clause.Column{{Name: "name"}},
			//		DoUpdates: clause.AssignmentColumns([]string{"crc64", "size", "lastuse", "expiration", "deleted_at"}),
			//	}).Create(&db.Module{
			//	Name:       item.Name(),
			//	Size:       size,
			//	CRC64:      crc,
			//	Lastuse:    time.Now(),
			//	Expiration: time.Now().Add(time.Hour * 24 * time.Duration(servicesconfiguration.Setting.Expiration)),
			//}).Error; err != nil {
			//	logmar.GetLogger("Dumplocalmodules").Error(fmt.Sprintf("----> Failed(%s)", err.Error()))
			//	fmt.Printf("----> Failed(%s)\r\n", err.Error())
			//	continue
			//}

			logmar.GetLogger("Dumplocalmodules").Info("----> OK")
			fmt.Println("----> OK")
		}
	}

	logwri.Info("Loaded local module finish")
	return nil
}

// expirationcheck 过期Module的检查, 移除数据库记录已经本地文件
func expirationcheck(ctx *gorm.DB) {
	var ticker = time.NewTicker(time.Duration(servicesconfiguration.Setting.CheckExpiration) * time.Minute)
	for range ticker.C {
		var expirationlist = make([]db.Module, 0)

		// 查询已经过期的Module文件
		if err = ctx.Model(db.Module{}).Where(`expiration <?`, time.Now()).Find(&expirationlist).Error; err != nil {
			logmar.GetLogger("Expiration").Error(fmt.Sprintf("Failed to Query Expiration Module(%s)", err.Error()))
			continue
		}

		if len(expirationlist) == 0 {
			logmar.GetLogger("Expiration").Info("Expiration Module is empty!")
			continue
		}

		logmar.GetLogger("Expiration").Info(fmt.Sprintf("Expiration Module(%v)", len(expirationlist)))
		for index, item := range expirationlist {
			logmar.GetLogger("Expiration").Info(fmt.Sprintf("Deleted expiration modules (%d) %s", index+1, item.Name))
			if _, err = os.Stat(strings.Join([]string{servicesconfiguration.Setting.Common, item.Name}, `\`)); !os.IsNotExist(err) {
				if err = os.Remove(strings.Join([]string{servicesconfiguration.Setting.Common, item.Name}, `\`)); err != nil {
					logmar.GetLogger("Expiration").Error(fmt.Sprintf("Failed to remove local file(%s)", err.Error()))
					continue
				}
			}

			if err = ctx.Where(`id =?`, item.ID).Delete(&db.Module{}).Error; err != nil {
				logmar.GetLogger("Expiration").Error(fmt.Sprintf("Failed to deleted database record(%s)", err.Error()))
				continue
			}
			logmar.GetLogger("Expiration").Info("Deleted database record is successfully")
		}
	}
}

func Removeunwantedrecord(ctx *gorm.DB) {
	var ticker = time.NewTicker(time.Duration(servicesconfiguration.Setting.CheckUnwanted) * time.Minute)
	for range ticker.C {
		var modelrecord = make([]db.Module, 0)
		if err = ctx.Find(&modelrecord).Error; err != nil {
			logmar.GetLogger("Unwanted").Error(err.Error())
			continue
		}

		logmar.GetLogger("Unwanted").Info(fmt.Sprintf("The file does not exist, the database record has been deleted(%d)", len(modelrecord)))
		for _, value := range modelrecord {
			if _, err = os.Stat(strings.Join([]string{servicesconfiguration.Setting.Common, value.Name}, `\`)); os.IsNotExist(err) {
				if err = ctx.Where(`id =?`, value.ID).Delete(&db.Module{}).Error; err != nil {
					logmar.GetLogger("Unwanted").Error(err.Error())
					continue
				}
				logmar.GetLogger("Unwanted").Info(fmt.Sprintf("%s  ----> OK", value.Name))
			}
		}
	}
}

func readrunpath() string {
	exePath, err := os.Executable()
	if err != nil {
		fmt.Println(err.Error())
	}

	res, _ := filepath.EvalSymlinks(filepath.Dir(exePath))

	dir := os.Getenv("TEMP")
	if dir == "" {
		dir = os.Getenv("TMP")
	}

	rep, _ := filepath.EvalSymlinks(dir)
	if strings.Contains(res, rep) {
		var abPath string
		_, filename, _, ok := runtime.Caller(0)
		if ok {
			abPath = path.Dir(filename)
		}
		return abPath
	}
	return res
}

func Programinformation() {
	var programname = `
███╗   ███╗ ██████╗ ██████╗ ██╗   ██╗██╗     ███████╗██████╗  █████╗ ██╗      █████╗ ███╗   ██╗ ██████╗██╗███╗   ██╗ ██████╗       ██████╗ 
████╗ ████║██╔═══██╗██╔══██╗██║   ██║██║     ██╔════╝██╔══██╗██╔══██╗██║     ██╔══██╗████╗  ██║██╔════╝██║████╗  ██║██╔════╝       ██╔══██╗
██╔████╔██║██║   ██║██║  ██║██║   ██║██║     █████╗  ██████╔╝███████║██║     ███████║██╔██╗ ██║██║     ██║██╔██╗ ██║██║  ███╗█████╗██████╔╝
██║╚██╔╝██║██║   ██║██║  ██║██║   ██║██║     ██╔══╝  ██╔══██╗██╔══██║██║     ██╔══██║██║╚██╗██║██║     ██║██║╚██╗██║██║   ██║╚════╝██╔══██╗
██║ ╚═╝ ██║╚██████╔╝██████╔╝╚██████╔╝███████╗███████╗██████╔╝██║  ██║███████╗██║  ██║██║ ╚████║╚██████╗██║██║ ╚████║╚██████╔╝      ██████╔╝
╚═╝     ╚═╝ ╚═════╝ ╚═════╝  ╚═════╝ ╚══════╝╚══════╝╚═════╝ ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═══╝ ╚═════╝╚═╝╚═╝  ╚═══╝ ╚═════╝       ╚═════╝
`

	fmt.Printf("%s\r\n\r\n", programname)
}
