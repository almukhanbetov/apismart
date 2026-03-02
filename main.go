package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Tariff struct {
	ID   int64   `json:"id"`
	Name string  `json:"name"`
	Sum  float64 `json:"sum"`
	Type int     `json:"type"`
}

type CheckResp struct {
	TxnID   string   `json:"txn_id"`
	Result  int      `json:"result"`
	Comment string   `json:"comment,omitempty"`
	Account string   `json:"account,omitempty"`
	BIN     string   `json:"bin,omitempty"`
	Tariffs []Tariff `json:"tariffs,omitempty"`
}

type PayReq struct {
	Account string  `json:"account"`
	TxnID   string  `json:"txn_id"`
	Sum     float64 `json:"sum"`
}

type PayResp struct {
	TxnID   string `json:"txn_id"`
	PrvTxn  int64  `json:"prv_txn,omitempty"`
	Result  int    `json:"result"`
	Comment string `json:"comment,omitempty"`
	BIN     string `json:"bin,omitempty"`
}

type MQTTReq struct {
	Command string  `json:"command"`
	TxnID   string  `json:"txn_id"`
	Account int64   `json:"account"`
	Sum     float64 `json:"sum,omitempty"`
}

var (
	db *sql.DB
	mq mqtt.Client

	mu            sync.RWMutex
	waitCheckByID = map[string]chan map[string]any{}
	waitPayByID   = map[string]chan map[string]any{}
)

const httpTimeout = 12 * time.Second

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing env: %s", key)
	}
	return v
}

func initDB() {
	dsn := mustEnv("MYSQL_DSN")
	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}
}

func initMQTT() {
	broker := mustEnv("MQTT_BROKER")
	user := mustEnv("MQTT_USER")
	pass := mustEnv("MQTT_PASS")

	opts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetUsername(user).
		SetPassword(pass).
		SetClientID("wash_api_" + strconv.FormatInt(time.Now().Unix(), 10)).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetCleanSession(false).
		SetKeepAlive(60 * time.Second).
		SetPingTimeout(10 * time.Second)

	mq = mqtt.NewClient(opts)
	if t := mq.Connect(); t.Wait() && t.Error() != nil {
		log.Fatal(t.Error())
	}

	// ответы от устройства
	if t := mq.Subscribe("payment/check", 1, onCheckMsg); t.Wait() && t.Error() != nil {
		log.Fatal(t.Error())
	}
	if t := mq.Subscribe("payment/pay", 1, onPayMsg); t.Wait() && t.Error() != nil {
		log.Fatal(t.Error())
	}
}

func onCheckMsg(_ mqtt.Client, msg mqtt.Message) {
	var m map[string]any
	if err := json.Unmarshal(msg.Payload(), &m); err != nil {
		return
	}
	txn, _ := m["txn_id"].(string)
	if txn == "" {
		return
	}
	mu.RLock()
	ch := waitCheckByID[txn]
	mu.RUnlock()
	if ch != nil {
		select { case ch <- m: default: }
	}
}

func onPayMsg(_ mqtt.Client, msg mqtt.Message) {
	var m map[string]any
	if err := json.Unmarshal(msg.Payload(), &m); err != nil {
		return
	}
	txn, _ := m["txn_id"].(string)
	if txn == "" {
		return
	}
	mu.RLock()
	ch := waitPayByID[txn]
	mu.RUnlock()
	if ch != nil {
		select { case ch <- m: default: }
	}
}

func accountInfo(account string) (exists bool, bin string, devType int, err error) {
	// Берём bin и type
	err = db.QueryRow(`SELECT COALESCE(bin,''), COALESCE(type,0) FROM devices WHERE account=? LIMIT 1`, account).
		Scan(&bin, &devType)
	if errors.Is(err, sql.ErrNoRows) {
		return false, "", 0, nil
	}
	if err != nil {
		return false, "", 0, err
	}
	return true, bin, devType, nil
}

func tariffsByType(devType int) ([]Tariff, error) {
	rows, err := db.Query(`SELECT id, tariff_id, name, sum, type FROM tariffs WHERE type=? ORDER BY sum ASC`, devType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Tariff
	for rows.Next() {
		var id int64
		var tariffID int64
		var name string
		var sum float64
		var t int
		if err := rows.Scan(&id, &tariffID, &name, &sum, &t); err != nil {
			return nil, err
		}
		_ = tariffID // если хочешь вернуть tariff_id — добавим поле
		out = append(out, Tariff{ID: id, Name: name, Sum: sum, Type: t})
	}
	return out, nil
}

func txnExists(txnID string) (bool, error) {
	var c int
	err := db.QueryRow(`SELECT COUNT(*) FROM payments WHERE txn_id=?`, txnID).Scan(&c)
	return c > 0, err
}

func savePayment(txnID, account string, sum float64, result int, comment string) (int64, error) {
	res, err := db.Exec(`INSERT INTO payments (txn_id, account, sum, result, comment) VALUES (?,?,?,?,?)`,
		txnID, account, sum, result, comment)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func mqttAsk(command, txnID, account string, sum float64, timeout time.Duration) (map[string]any, error) {
	accInt, err := strconv.ParseInt(account, 10, 64)
	if err != nil {
		return nil, errors.New("account must be number")
	}
	req := MQTTReq{Command: command, TxnID: txnID, Account: accInt, Sum: sum}
	b, _ := json.Marshal(req)

	ch := make(chan map[string]any, 1)
	if command == "check" {
		mu.Lock()
		waitCheckByID[txnID] = ch
		mu.Unlock()
		defer func() {
			mu.Lock()
			delete(waitCheckByID, txnID)
			mu.Unlock()
			close(ch)
		}()
	} else {
		mu.Lock()
		waitPayByID[txnID] = ch
		mu.Unlock()
		defer func() {
			mu.Lock()
			delete(waitPayByID, txnID)
			mu.Unlock()
			close(ch)
		}()
	}

	// куда публикуем команду устройству
	mq.Publish("payment/app", 1, false, b)

	select {
	case resp := <-ch:
		return resp, nil
	case <-time.After(timeout):
		return nil, errors.New("device timeout")
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	initDB()
	defer db.Close()

	initMQTT()
	defer mq.Disconnect(250)

	r := gin.Default()

	r.GET("/api/check", func(c *gin.Context) {
		txnID := c.Query("txn_id")
		account := c.Query("account")
		if txnID == "" || account == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "missing txn_id or account"})
			return
		}

		ok, bin, devType, err := accountInfo(account)
		if err != nil {
			c.JSON(500, CheckResp{TxnID: txnID, Result: 1, Comment: "db error"})
			return
		}
		if !ok {
			c.JSON(200, CheckResp{TxnID: txnID, Result: 1, Comment: "account not found"})
			return
		}

		resp, err := mqttAsk("check", txnID, account, 0, httpTimeout)
		if err != nil {
			c.JSON(200, CheckResp{TxnID: txnID, Result: 1, Comment: err.Error()})
			return
		}

		// result из MQTT
		result := 1
		if v, ok := resp["result"]; ok {
			switch t := v.(type) {
			case float64:
				result = int(t)
			case int:
				result = t
			}
		}

		if result != 0 {
			c.JSON(200, CheckResp{TxnID: txnID, Result: 1, Comment: "device check failed"})
			return
		}

		tariffs, err := tariffsByType(devType)
		if err != nil {
			c.JSON(200, CheckResp{TxnID: txnID, Result: 1, Comment: "tariffs error"})
			return
		}

		c.JSON(200, CheckResp{
			TxnID:   txnID,
			Result:  0,
			Comment: "OK",
			Account: account,
			BIN:     bin,
			Tariffs: tariffs,
		})
	})

	r.POST("/api/pay", func(c *gin.Context) {
		var req PayReq
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": "bad json"})
			return
		}
		if req.Account == "" || req.TxnID == "" || req.Sum <= 0 {
			c.JSON(400, gin.H{"error": "account, txn_id, sum required"})
			return
		}

		dup, err := txnExists(req.TxnID)
		if err != nil {
			c.JSON(200, PayResp{TxnID: req.TxnID, Result: 1, Comment: "db error"})
			return
		}
		if dup {
			c.JSON(200, PayResp{TxnID: req.TxnID, Result: 3, Comment: "txn_id duplicate"})
			return
		}

		ok, bin, _, err := accountInfo(req.Account)
		if err != nil || !ok {
			c.JSON(200, PayResp{TxnID: req.TxnID, Result: 1, Comment: "account not found"})
			return
		}

		resp, err := mqttAsk("pay", req.TxnID, req.Account, req.Sum, httpTimeout)
		if err != nil {
			c.JSON(200, PayResp{TxnID: req.TxnID, Result: 1, Comment: err.Error()})
			return
		}

		result := 1
		comment := "pay failed"
		if v, ok := resp["result"]; ok {
			switch t := v.(type) {
			case float64:
				result = int(t)
			case int:
				result = t
			}
		}
		if s, ok := resp["comment"].(string); ok && s != "" {
			comment = s
		}
		if result == 0 {
			comment = "OK"
		}

		prvTxn, err := savePayment(req.TxnID, req.Account, req.Sum, result, comment)
		if err != nil {
			c.JSON(200, PayResp{TxnID: req.TxnID, Result: 1, Comment: "save payment error"})
			return
		}

		c.JSON(200, PayResp{
			TxnID:   req.TxnID,
			PrvTxn:  prvTxn,
			Result:  result,
			Comment: comment,
			BIN:     bin,
		})
	})

	// healthcheck
	r.GET("/health", func(c *gin.Context) { c.JSON(200, gin.H{"ok": true}) })

	srv := &http.Server{
		Addr:         ":8080",
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	// аккуратное выключение (по желанию)
	go func() {
		log.Println("Gin API on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	// блокируемся навсегда в простом варианте
	select {}
	// если хочешь graceful shutdown через os.Signal — добавлю
	_ = context.Background()
}