/*
*

	@author: kiki
	@since: 2025/5/25
	@desc: //TODO

*
*/

package db

import (
	"time"

	"gorm.io/gorm"
)

//type AOD struct {
//	gorm.Model
//	Name       string    `gorm:"type:varchar(30);column:name;unique" json:"name"`
//	Size       int64     `gorm:"column:size;default:0" json:"size"`
//	Lastuse    time.Time `gorm:"type:datetime;column:lastuse" json:"lastuse"`
//	Expiration time.Time `gorm:"type:datetime;column:expiration" json:"expiration"`
//	Content    []byte    `gorm:"type:longtext;column:content" json:"content"`
//}

type Module struct {
	gorm.Model
	CRC64      uint64    `gorm:"column:crc64;not null"`
	Name       string    `gorm:"type:varchar(255);column:name;unique" json:"name"`
	Size       int64     `gorm:"column:size;default:0" json:"size"`
	Lastuse    time.Time `gorm:"type:datetime;column:lastuse" json:"lastuse"`
	Expiration time.Time `gorm:"type:datetime;column:expiration" json:"expiration"`
}

func AutoMigrate() []any {
	return []any{&Module{}}
}
