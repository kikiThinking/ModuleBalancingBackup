/*
*

	@author: kiki
	@since: 2025/5/26
	@desc: //TODO

*
*/

package api

import (
	"ModuleBalancingbackupservice/db"
	"ModuleBalancingbackupservice/env"
	rpc "ModuleBalancingbackupservice/grpc"
	"ModuleBalancingbackupservice/logmanager"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redmask-hb/GoSimplePrint/goPrint"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

var err error

type ModuleBalancing struct {
	rpc.UnimplementedModuleServer
	Configuration *env.Configuration
	Dbcontrol     *gorm.DB
	Logmar        *logmanager.LogManager
}

// Push 客户端下载请求
func (the *ModuleBalancing) Push(request *rpc.ModuleDownloadRequest, stream rpc.Module_PushServer) error {
	the.Logmar.GetLogger("ClientDownload").Info(fmt.Sprintf("Client(%s) requests to download module(%s)", request.Serveraddress, request.Filename))
	if strings.EqualFold(request.Filename, "") {
		return errors.New("filename can not be empty")
	}

	var module = new(db.Module)
	var fp = strings.Join([]string{the.Configuration.Setting.Common, request.Filename}, `\`)

	var exist bool
	if err = the.Dbcontrol.Model(db.Module{}).Select("COUNT(*) > 0").Where(db.Module{Name: request.Filename}).Scan(&exist).Error; err != nil {
		the.Logmar.GetLogger("ClientDownload").Error(fmt.Sprintf("failed to check databse record(%s) error(%s)", request.Filename, err.Error()))
		return fmt.Errorf("failed to check databse record(%s) error(%s)", request.Filename, err.Error())
	}

	if exist {
		if _, err = os.Stat(fp); os.IsNotExist(err) {
			the.Logmar.GetLogger("ClientDownload").Error(fmt.Sprintf("There are records in the database, but entity file do not exist and may have been manually deleted(%s)", request.Filename))
			if err = the.Dbcontrol.Where(db.Module{Name: request.Filename}).Delete(&module).Error; err != nil {
				return err
			}

			return fmt.Errorf("there are records in the database, but entity file do not exist and may have been manually deleted(%s)", request.Filename)
		}
	} else {
		if _, err = os.Stat(fp); os.IsNotExist(err) { // 判断Module是否在本地
			the.Logmar.GetLogger("ClientDownload").Error(fmt.Sprintf("The server does not have such a file(%s)", request.Filename))
			return fmt.Errorf("the server does not have such a file(%s)", request.Filename)
		} else { // 解析本地文件
			var crc uint64
			var size int64
			if crc, size, err = env.CRC64(fp, 128*1024*1024, 8); err != nil {
				return err
			}

			if err = the.Dbcontrol.Model(db.Module{}).Create(&db.Module{
				CRC64:      crc,
				Name:       filepath.Base(fp),
				Size:       size,
				Lastuse:    time.Now(),
				Expiration: time.Now().Add(time.Hour * 24 * time.Duration(the.Configuration.Setting.Expiration)),
			}).Error; err != nil {
				return err
			}
		}
	}

	if err = the.Dbcontrol.Model(db.Module{}).Where(db.Module{Name: request.Filename}).First(&module).Error; err != nil {
		return err
	}

	the.Logmar.GetLogger("ClientDownload").Info(fmt.Sprintf("Hit  ID: %-5d  Name: %-20s  CRC: %-20v  Size: %-20v  Lastuse: %-20s  Expiration: %-20s",
		module.ID,
		module.Name,
		module.CRC64,
		module.Size,
		module.Lastuse.Format(`2006-01-02 15:04:05`),
		module.Expiration.Format(`2006-01-02 15:04:05`)))

	the.Logmar.GetLogger("ClientDownload").Info(fmt.Sprintf("fp(%s) seek(%v)", fp, request.Offset))
	var offset = request.Offset
	f, err := os.OpenFile(fp, os.O_RDONLY, 0644)
	if err != nil {
		the.Logmar.GetLogger("ClientDownload").Error(fmt.Sprintf("failed to open file: %s", err.Error()))
		return err
	}

	defer f.Close()

	finformation, err := f.Stat()
	if err != nil {
		the.Logmar.GetLogger("ClientDownload").Error(fmt.Sprintf("failed to read file stat: %s", err.Error()))
		return err
	}

	var fcreatedate int64
	switch runtime.GOOS {
	case "windows":
		fcreatedate = finformation.Sys().(*syscall.Win32FileAttributeData).CreationTime.Nanoseconds() / 1e9
	default:
		fcreatedate = time.Now().Unix()
	}

	if err = stream.SendHeader(metadata.New(map[string]string{
		"size":     strconv.FormatInt(finformation.Size(), 10),
		"filename": module.Name,
		"crc64":    strconv.FormatUint(module.CRC64, 10),
		"munix":    strconv.FormatInt(finformation.ModTime().Unix(), 10),
		"cunix":    strconv.FormatInt(fcreatedate, 10),
	})); err != nil {
		the.Logmar.GetLogger("ClientDownload").Error(fmt.Sprintf("failed to send headers: %s", err.Error()))
		return err
	}

	if offset > 0 {
		_, err := f.Seek(offset, io.SeekStart)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to seek file: %v", err)
		}
	}

	var buffer = make([]byte, 1*1024*1024) // 1MB

	for {
		number, err := f.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break // 文件读取完毕
			}
			return status.Errorf(codes.Internal, "failed to read file: %v", err)
		}

		if err = stream.Send(&rpc.ModulePushResponse{
			Content:   buffer[:number],
			Completed: false,
		}); err != nil {
			return status.Errorf(codes.Internal, "failed to send chunk: %v", err)
		}
	}

	if err = stream.Send(&rpc.ModulePushResponse{
		Content:   []byte{},
		Completed: true,
	}); err != nil {
		return status.Errorf(codes.Internal, "failed to send chunk: %v", err)
	}

	if err = the.Dbcontrol.Where(`id = ?`, module.ID).Delete(&db.Module{}).Error; err != nil {
		return status.Errorf(codes.Internal, "failed to delete database record %s: %v", request.Filename, err)
	}

	if err = os.Remove(fp); err != nil {
		return status.Errorf(codes.Internal, "failed to delete file %s: %v", fp, err)
	}

	return nil
}

func (the *ModuleBalancing) Upload(stream rpc.Module_UploadServer) error {
	var (
		uploadinformation *rpc.Finformation
		f                 *os.File
		size              int64
		bar               *goPrint.Bar
	)

	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			f.Close()
			bar.PrintEnd("\t\t ----> Upload end")
			var (
				CRC   uint64
				lsize int64
			)

			if CRC, lsize, err = env.CRC64(strings.Join([]string{the.Configuration.Setting.Common, uploadinformation.Filename}, `\`), 128*1024*1024, 8); err != nil {
				the.Logmar.GetLogger("Upload").Error(fmt.Sprintf("Failed to calculate CRC64: %s", err.Error()))
				return stream.SendAndClose(&rpc.UploadResponse{
					Message: err.Error(),
					Success: false,
				})
			}

			if !strings.EqualFold(strconv.FormatUint(CRC, 10), uploadinformation.Crc64) || !strings.EqualFold(strconv.FormatInt(size, 10), uploadinformation.Size) {
				the.Logmar.GetLogger("Upload").Error(fmt.Sprintf("Error: Upload failed, Size or CRC mismatch, CRC(Local(%v) %s) Size(Local(%v) %s)", CRC, uploadinformation.Crc64, lsize, uploadinformation.Size))
				return stream.SendAndClose(&rpc.UploadResponse{
					Message: fmt.Sprintf("Error: Upload failed, Size or CRC mismatch, CRC(Local(%v) %s) Size(Local(%v) %s)", CRC, uploadinformation.Crc64, lsize, uploadinformation.Size),
					Success: false,
				})
			}

			var (
				Cunix int64
				Munix int64
			)

			if Cunix, err = strconv.ParseInt(uploadinformation.CUnix, 10, 64); err != nil {
				the.Logmar.GetLogger("Upload").Error(fmt.Sprintf("Failed to parseint(%s): %s", uploadinformation.CUnix, err.Error()))
				return stream.SendAndClose(&rpc.UploadResponse{
					Message: fmt.Sprintf("Failed to parseint(%s): %s", uploadinformation.CUnix, err.Error()),
					Success: false,
				})
			}

			if Munix, err = strconv.ParseInt(uploadinformation.MUnix, 10, 64); err != nil {
				the.Logmar.GetLogger("Upload").Error(fmt.Sprintf("Failed to parseint(%s): %s", uploadinformation.MUnix, err.Error()))
				return stream.SendAndClose(&rpc.UploadResponse{
					Message: fmt.Sprintf("Failed to parseint(%s): %s", uploadinformation.MUnix, err.Error()),
					Success: false,
				})
			}

			if err = env.Changefiletime(
				strings.Join([]string{the.Configuration.Setting.Common, uploadinformation.Filename}, `\`),
				Munix,
				Cunix,
			); err != nil {
				the.Logmar.GetLogger("Upload").Error(fmt.Sprintf("Failed to change file time: %s", err.Error()))
				return stream.SendAndClose(&rpc.UploadResponse{
					Message: err.Error(),
					Success: false,
				})
			}

			the.Logmar.GetLogger("Upload").Info(fmt.Sprintf("file upload complete information: %-20s CRC: %-20v  Size: %-10v  Create: %-20s  Modify: %-20s",
				uploadinformation.Filename,
				CRC,
				size,
				time.Unix(Cunix, 0).Format(`2006-01-02 15:04:05`),
				time.Unix(Munix, 0).Format(`2006-01-02 15:04:05`),
			))

			return stream.SendAndClose(&rpc.UploadResponse{
				Message: fmt.Sprintf("file(%s) upload successfully", uploadinformation.Filename),
				Success: true,
			})
		}

		if err != nil {
			the.Logmar.GetLogger("Upload").Error(fmt.Sprintf("Error receiving data: %v", err.Error()))
			return err
		}

		switch data := req.Data.(type) {
		case *rpc.UploadRequest_Information:
			uploadinformation = data.Information
			if f, err = os.Create(strings.Join([]string{the.Configuration.Setting.Common, uploadinformation.Filename}, `\`)); err != nil {
				the.Logmar.GetLogger("Upload").Error(fmt.Sprintf("Failed to create file: %s", err.Error()))
				return err
			}

			fsize, err := strconv.ParseInt(uploadinformation.Size, 10, 64)
			if err != nil {
				return stream.SendAndClose(&rpc.UploadResponse{
					Message: err.Error(),
					Success: false,
				})
			}

			bar = goPrint.NewBar(int(fsize) / 1024 / 1024)
			bar.SetGraph(`=`)
			bar.SetNotice("(MB)")
			bar.SetEnds("{", "}")

		case *rpc.UploadRequest_ChunkData:
			chunk := data.ChunkData
			bytesize, err := f.Write(chunk)
			if err != nil {
				log.Printf("Error writing to file: %v", err)
				return err
			}

			size += int64(bytesize)
			bar.PrintBar(int(size / 1024 / 1024))
		}
	}
}
