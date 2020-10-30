package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/p4gefau1t/trojan-go/config"

	// MySQL Driver
	_ "github.com/go-sql-driver/mysql"

	"github.com/p4gefau1t/trojan-go/common"
	"github.com/p4gefau1t/trojan-go/log"
	"github.com/p4gefau1t/trojan-go/statistic"
	"github.com/p4gefau1t/trojan-go/statistic/memory"
)

const Name = "MYSQL"

type Authenticator struct {
	*memory.Authenticator
	db             *sql.DB
	updateDuration time.Duration
	ctx            context.Context
}

func (a *Authenticator) updater() {
	for {
		for _, user := range a.ListUsers() {
			//swap upload and download for users
			hash := user.Hash()
			sent, recv := user.ResetTraffic()

			if sent == 0 && recv == 0 { // 都为0则不需要记录流量
				continue
			}

			s, err := a.db.Exec("UPDATE `users` SET `upload`=`upload`+?, `download`=`download`+? WHERE `password`=?;", recv, sent, hash)
			if err != nil {
				log.Error(common.NewError("failed to update data to user table").Base(err))
				continue
			}
			r, err := s.RowsAffected()
			if err != nil {
				log.Error(common.NewError("failed to update data to user table").Base(err))
				if r == 0 {
					a.DelUser(hash)
				}
				continue
			}
		}
		log.Info("buffered data has been written into the database")

		//update memory
		rows, err := a.db.Query("SELECT password,quota,download,upload,status FROM users")
		if err != nil {
			log.Error(common.NewError("failed to pull data from the database").Base(err))
			time.Sleep(a.updateDuration)
			continue
		}
		for rows.Next() {
			var hash string
			var quota, download, upload, status int64
			err := rows.Scan(&hash, &quota, &download, &upload, &status)
			if err != nil {
				log.Error(common.NewError("failed to obtain data from the query result").Base(err))
				break
			}
			log.Debug("状态改为了", hash, status)
			if status == 1 {
				log.Debug("删除了:", hash)
				a.DelUser(hash)
				continue
			}

			if download+upload < quota || quota < 0 {
				log.Debug("添加了hash", hash)
				a.AddUser(hash)
			} else {
				a.DelUser(hash)
			}
		}

		select {
		case <-time.After(a.updateDuration):
		case <-a.ctx.Done():
			log.Debug("MySQL daemon exiting...")
			return
		}
	}
}

func connectDatabase(driverName, username, password, ip string, port int, dbName string) (*sql.DB, error) {
	path := strings.Join([]string{username, ":", password, "@tcp(", ip, ":", fmt.Sprintf("%d", port), ")/", dbName, "?charset=utf8"}, "")
	return sql.Open(driverName, path)
}

func NewAuthenticator(ctx context.Context) (statistic.Authenticator, error) {
	cfg := config.FromContext(ctx, Name).(*Config)
	db, err := connectDatabase(
		"mysql",
		cfg.MySQL.Username,
		cfg.MySQL.Password,
		cfg.MySQL.ServerHost,
		cfg.MySQL.ServerPort,
		cfg.MySQL.Database,
	)
	if err != nil {
		return nil, common.NewError("Failed to connect to database server").Base(err)
	}
	memoryAuth, err := memory.NewAuthenticator(ctx)
	if err != nil {
		return nil, err
	}
	a := &Authenticator{
		db:             db,
		ctx:            ctx,
		updateDuration: time.Duration(cfg.MySQL.CheckRate) * time.Second,
		Authenticator:  memoryAuth.(*memory.Authenticator),
	}
	go a.updater()
	log.Debug("mysql authenticator created")
	return a, nil
}

func init() {
	statistic.RegisterAuthenticatorCreator(Name, NewAuthenticator)
}
