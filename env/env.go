/*
*

	@author: kiki
	@since: 2025/5/28
	@desc: //TODO

*
*/

package env

import (
	"ModuleBalancingbackupservice/db"
	"ModuleBalancingbackupservice/logmanager"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/rjeczalik/notify"
	"golang.org/x/sys/windows"
	"gorm.io/gorm"
)

type Configuration struct {
	Setting struct {
		Expiration      int64  `yaml:"Expiration"`
		CheckExpiration int64  `yaml:"CheckExpiration"`
		CheckUnwanted   int64  `yaml:"CheckUnwanted"`
		ReserveSize     int64  `yaml:"ReserveSize"`
		Common          string `yaml:"Common"`
	} `yaml:"Setting"`
	Database struct {
		Host     string `yaml:"Host"`
		Port     string `yaml:"Port"`
		Username string `yaml:"Username"`
		Password string `yaml:"Password"`
	} `yaml:"Database"`
	GRPC struct {
		Port string `yaml:"Port"`
	} `yaml:"GRPC"`
}

var (
	err error
)

func CRC64(filePath string, chunkSize int64, workers int) (uint64, int64, error) {
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to open file: %v", err)
	}
	defer f.Close()

	finformation, err := f.Stat()
	if err != nil {
		return 0, 0, fmt.Errorf("filed to get file information: %v", err)
	}

	fileSize := finformation.Size()
	if fileSize == 0 {
		return 0, finformation.Size(), err
	}

	chunks := int((fileSize + chunkSize - 1) / chunkSize)
	if workers > chunks {
		workers = chunks
	}

	table := crc64.MakeTable(crc64.ECMA)
	var wg sync.WaitGroup
	results := make([]uint64, chunks) // 使用切片存储结果，保持顺序
	workCh := make(chan int, chunks)
	errCh := make(chan error, 1)
	var hasError bool
	var mu sync.Mutex // 用于保护hasError

	// 启动worker池
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunkIndex := range workCh {
				offset := int64(chunkIndex) * chunkSize
				size := chunkSize
				if offset+size > fileSize {
					size = fileSize - offset
				}

				buf := make([]byte, size)
				n, err := f.ReadAt(buf, offset)
				if err != nil && err != io.EOF {
					mu.Lock()
					if !hasError {
						errCh <- fmt.Errorf("读取文件块 %d 失败: %v", chunkIndex, err)
						hasError = true
					}
					mu.Unlock()
					return
				}

				// 计算当前块的CRC
				sum := crc64.Checksum(buf[:n], table)

				// 直接存储到结果切片的对应位置
				results[chunkIndex] = sum
			}
		}()
	}

	// 分发任务
	go func() {
		for i := 0; i < chunks; i++ {
			mu.Lock()
			if hasError {
				mu.Unlock()
				break
			}
			mu.Unlock()
			workCh <- i
		}
		close(workCh)
	}()

	// 等待完成
	go func() {
		wg.Wait()
		close(errCh) // 关闭错误通道表示所有工作完成
	}()

	// 处理错误
	if err := <-errCh; err != nil {
		return 0, finformation.Size(), err
	}

	// 按顺序合并所有块的CRC值
	finalCRC := uint64(0)
	for i := 0; i < chunks; i++ {
		sum := results[i]
		finalCRC = crc64.Update(finalCRC, table, []byte{
			byte(sum >> 56), byte(sum >> 48), byte(sum >> 40), byte(sum >> 32),
			byte(sum >> 24), byte(sum >> 16), byte(sum >> 8), byte(sum),
		})
	}

	return finalCRC, finformation.Size(), nil
}

func Monitornewmodule(ctx *gorm.DB, logwri *logmanager.BusinessLogger, expiration int64, monitorpath string) {
	logwri.Info(fmt.Sprintf("Start listening to the local Module directory(%s)", monitorpath))
	var monitorfile = make(chan string, 100)
	var longwaitchan = make(chan string, 20)
	go func() {
		var ticker = time.NewTicker(time.Minute)
		for {
			select {
			case fp := <-longwaitchan:
				logwri.Info(fmt.Sprintf("Create a long waiting task: %s", fp))
				var checkn = 0
				for range ticker.C {
					if checkn == 30 {
						break
					}
					// 0 --> 以独占模式尝试打开文件来判断文件是否写入完成
					if handle, err := windows.CreateFile(windows.StringToUTF16Ptr(fp), windows.GENERIC_READ, 0, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0); err != nil {
						checkn++
						continue
					} else {
						_ = windows.CloseHandle(handle)
						monitorfile <- fp
						break
					}

				}
			}
		}
	}()

	go func() {
		for {
			select {
			case fp := <-monitorfile:
				logwri.Info(fmt.Sprintf("Create a loading task, waiting for file writing to complete(%s)", fp))
				var ticker = time.NewTicker(time.Second * 5)
				var loop = 0
				var fclose = false
				for range ticker.C {
					if loop == 10 {
						break
					}

					// 0 --> 以独占模式尝试打开文件来判断文件是否写入完成
					if handle, err := windows.CreateFile(windows.StringToUTF16Ptr(fp), windows.GENERIC_READ, 0, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0); err != nil {
						loop++
						continue
					} else {
						_ = windows.CloseHandle(handle)
						fclose = true
						break
					}
				}

				if fclose {
					var (
						crc  uint64
						size int64
					)

					if crc, size, err = CRC64(fp, 128*1024*1024, 8); err != nil {
						logwri.Error(err.Error())
						continue
					}

					var module = db.Module{
						CRC64:      crc,
						Name:       filepath.Base(fp),
						Size:       size,
						Lastuse:    time.Now(),
						Expiration: time.Now().Add(time.Hour * 24 * time.Duration(expiration)),
					}

					// 如果有记录就更新没有就创建
					var isexistrecord bool
					if err = ctx.Unscoped().Model(db.Module{}).Select(`COUNT(*) > 0`).Where(db.Module{Name: module.Name}).Scan(&isexistrecord).Error; err != nil {
						logwri.Error(err.Error())
						continue
					}

					if isexistrecord {
						if err = ctx.Unscoped().Model(db.Module{}).Where(db.Module{Name: module.Name}).
							Updates(map[string]interface{}{
								"crc64":      module.CRC64,
								"size":       module.Size,
								"lastuse":    module.Lastuse,
								"expiration": module.Expiration,
								"deleted_at": nil,
							}).Error; err != nil {
							logwri.Error(err.Error())
							continue
						}
					} else {
						if err = ctx.Model(db.Module{}).Create(&module).Error; err != nil {
							logwri.Error(err.Error())
							continue
						}
					}

					//if err = ctx.Clauses(
					//	clause.OnConflict{
					//		Columns:   []clause.Column{{Name: "name"}},
					//		DoUpdates: clause.AssignmentColumns([]string{"crc64", "size", "lastuse", "expiration", "deleted_at"}),
					//	}).Create(&module).Error; err != nil {
					//	logwri.Error(err.Error())
					//	continue
					//}

					logwri.Info(fmt.Sprintf("Create a new module record ----> %-20s  Size: %-10v  CRC64: %-20v  Lastuse:%-20s  Expiration:%-20s",
						module.Name,
						module.Size,
						module.CRC64,
						module.Lastuse.Format(`2006-01-02 15:04:05`),
						module.Expiration.Format(`2006-01-02 15:04:05`),
					))

				} else {
					logwri.Error(fmt.Sprintf("The file has been occupied for more than 10 minutes(%s))", fp))
				}

			}
		}
	}()

	var (
		monitorchannel = make(chan notify.EventInfo, 100)
		monitorevent   = notify.Create
	)

	if err := notify.Watch(monitorpath, monitorchannel, monitorevent); err != nil {
		logwri.Error(err.Error())
		return
	}

	defer notify.Stop(monitorchannel)

	for {
		select {
		case mp := <-monitorchannel:
			switch mp.Event() {
			case notify.Create:
				logwri.Info(fmt.Sprintf("Detected newly added projects(%s)", mp.Path()))
				inf, err := os.Stat(mp.Path())
				if err != nil {
					logwri.Error("The monitored file does not exist")
					continue
				}

				if inf.IsDir() {
					continue
				}

				monitorfile <- mp.Path()
			default:
			}
		}
	}
}

func MonitornewmoduleBack(ctx *gorm.DB, logwri *logmanager.BusinessLogger, expiration int64, monitorpath string) {
	logwri.Info(fmt.Sprintf("starting monitor ----> (%s)", monitorpath))
	var monitorfile = make(chan string, 200)
	go func() {
		for {
			select {
			case fp := <-monitorfile:
				var ticker = time.NewTicker(time.Second * 5)
				var loop = 0
				var fclose = false
				for range ticker.C {
					if loop == 120 {
						break
					}
					if handle, err := windows.CreateFile(
						windows.StringToUTF16Ptr(fp),
						windows.GENERIC_READ,
						0, // 关键：共享模式为0，表示独占访问
						nil,
						windows.OPEN_EXISTING,
						windows.FILE_ATTRIBUTE_NORMAL,
						0,
					); err != nil {
						loop++
						continue
					} else {
						_ = windows.CloseHandle(handle)
						fclose = true
						break
					}
				}

				if fclose {
					var (
						crc  uint64
						size int64
					)

					if crc, size, err = CRC64(fp, 128*1024*1024, 8); err != nil {
						logwri.Error(err.Error())
						continue
					}

					var module = db.Module{
						CRC64:      crc,
						Name:       filepath.Base(fp),
						Size:       size,
						Lastuse:    time.Now(),
						Expiration: time.Now().Add(time.Hour * 24 * time.Duration(expiration)),
					}

					var isexistrecord bool
					if err = ctx.Unscoped().Model(db.Module{}).Select(`COUNT(*) > 0`).Where(db.Module{Name: module.Name}).Scan(&isexistrecord).Error; err != nil {
						logwri.Error(err.Error())
						continue
					}

					if isexistrecord {
						if err = ctx.Unscoped().Model(db.Module{}).Where(db.Module{Name: module.Name}).
							Updates(map[string]interface{}{
								"crc64":      module.CRC64,
								"size":       module.Size,
								"lastuse":    module.Lastuse,
								"expiration": module.Expiration,
								"deleted_at": nil,
							}).Error; err != nil {
							logwri.Error(err.Error())
							continue
						}
					} else {
						if err = ctx.Model(db.Module{}).Create(&module).Error; err != nil {
							logwri.Error(err.Error())
							continue
						}
					}

					logwri.Info(fmt.Sprintf("Create a new module record ----> %-20s  Size: %-10v  CRC64: %-20v  Lastuse:%-20s  Expiration:%-20s",
						module.Name,
						module.Size,
						module.CRC64,
						module.Lastuse.Format(`2006-01-02 15:04:05`),
						module.Expiration.Format(`2006-01-02 15:04:05`),
					))

				} else {
					logwri.Error(fmt.Sprintf("The file has been occupied for more than 10 minutes(%s))", fp))
				}
			}
		}
	}()

	monitordir, err := fsnotify.NewWatcher()
	if err != nil {
		logwri.Error(fmt.Sprintf("Monitor Path NewWatcher Error: %s", err.Error()))
		return
	}

	if err = monitordir.Add(monitorpath); err != nil {
		logwri.Error(fmt.Sprintf("Add Monitor Path Error: %s", err.Error()))
		return
	}

	var number = 1
	for {
		select {
		case cre := <-monitordir.Events:
			if cre.Op&fsnotify.Create == fsnotify.Create {
				fmt.Printf("(%v) %s", number, filepath.Base(cre.Name))
				inf, err := os.Stat(cre.Name)
				if err != nil {
					logwri.Error(err.Error())
					continue
				}

				if inf.IsDir() {
					continue
				}

				if strings.Contains(cre.Name, "frombackdownload") {
					logwri.Info(fmt.Sprintf("file(%s) from backup server download, skip check!", cre.Name))
					continue
				}

				fmt.Println("\t ----> OK")
				monitorfile <- cre.Name
				number++
			}
		case err := <-monitordir.Errors:
			logwri.Error(fmt.Sprintf("Panic: Monitor Path: %s", err.Error()))
			continue
		}
	}
}

func Changefiletime(fp string, munix, cunix int64) error {
	var (
		handle   syscall.Handle
		uint16fp *uint16
	)
	// 转换文件路径为UTF-16指针
	if uint16fp, err = syscall.UTF16PtrFromString(fp); err != nil {
		return err
	}

	// 打开文件获取句柄
	if handle, err = syscall.CreateFile(
		uint16fp,
		syscall.FILE_WRITE_ATTRIBUTES,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE,
		nil,
		syscall.OPEN_EXISTING,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0,
	); err != nil {
		return err
	}

	ParseWindowsTime := func(t time.Time) syscall.Filetime {
		return syscall.NsecToFiletime(t.UnixNano())
	}
	Ctime := ParseWindowsTime(time.Unix(cunix, 0))
	Mtime := ParseWindowsTime(time.Unix(munix, 0))
	Rtime := ParseWindowsTime(time.Now())
	defer syscall.CloseHandle(handle)
	return syscall.SetFileTime(handle, &Ctime, &Rtime, &Mtime)
}

func GetDiskSpace(path string) (uint64, error) {
	var total, free, available uint64

	// Windows 系统
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}

	err = windows.GetDiskFreeSpaceEx(pathPtr, &available, &total, &free)
	if err != nil {
		return 0, err
	}

	return free, nil
}
