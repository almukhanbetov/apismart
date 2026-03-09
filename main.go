package main

import (
	"database/sql"
	"fmt"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
)

const MYSQL_DSN = "smart_db:Asetaset_97@tcp(127.0.0.1:3306)/smart_db"

var db *sql.DB

func main() {
	var err error
	db, err = sql.Open("mysql", MYSQL_DSN)
	if err != nil {
		panic(err)
	}
	err = db.Ping()
	if err != nil {
		panic(err)
	}
	fmt.Println("MYSQL CONNECTED")
	r := gin.Default()
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status": "ok",
		})
	})
	r.GET("/users", getUsers)
	r.GET("/report/devices", reportDevices)
	r.GET("/report/finance", reportFinance)
	r.GET("/report/payments", reportPayments)
	r.GET("/report/transactions", reportTransactions)
	r.GET("/report/status", reportStatus)

	r.Run("0.0.0.0:8182")
}
func getUsers(c *gin.Context) {
	rows, err := db.Query("SELECT id,phone,fullname FROM users")
	if err != nil {
		c.JSON(500, err.Error())
		return
	}
	defer rows.Close()
	type User struct {
		ID       int    `json:"id"`
		Phone    string `json:"phone"`
		Fullname string `json:"fullname"`
	}

	var users []User
	for rows.Next() {
		var u User
		rows.Scan(&u.ID, &u.Phone, &u.Fullname)
		users = append(users, u)
	}
	c.JSON(200, users)
}
func reportDevices(c *gin.Context) {
	rows, err := db.Query(`
SELECT
d.account,
MAX(d.device_name) as device_name,
MAX(u.fullname) as user,
MAX(ds.signal_wifi) as wifi_signal,

COUNT(DISTINCT t.id) as transactions_count,
IFNULL(SUM(t.sum),0) as transactions_sum,

COUNT(DISTINCT p.id) as payments_count,
IFNULL(SUM(p.sum),0) as payments_sum,

(IFNULL(SUM(p.sum),0) - IFNULL(SUM(t.sum),0)) as balance

FROM devices d

LEFT JOIN users u
ON u.user_code = d.user_code

LEFT JOIN device_set ds
ON ds.account = d.account

LEFT JOIN transactions t
ON t.account = d.account

LEFT JOIN payments p
ON p.account = d.account

GROUP BY d.account
`)
	if err != nil {
		c.JSON(500, err.Error())
		return
	}
	type DeviceReport struct {
		Account           int     `json:"account"`
		DeviceName        string  `json:"device_name"`
		User              string  `json:"user"`
		WifiSignal        int     `json:"wifi_signal"`
		TransactionsCount int     `json:"transactions_count"`
		TransactionsSum   float64 `json:"transactions_sum"`
		PaymentsCount     int     `json:"payments_count"`
		PaymentsSum       float64 `json:"payments_sum"`
		Balance           float64 `json:"balance"`
	}
	var result []DeviceReport
	for rows.Next() {
		var r DeviceReport
		rows.Scan(
			&r.Account,
			&r.DeviceName,
			&r.User,
			&r.WifiSignal,
			&r.TransactionsCount,
			&r.TransactionsSum,
			&r.PaymentsCount,
			&r.PaymentsSum,
			&r.Balance,
		)

		result = append(result, r)

	}

	c.JSON(200, result)
}
func reportFinance(c *gin.Context) {

	row := db.QueryRow(`
SELECT
COUNT(DISTINCT account),
COUNT(id),
IFNULL(SUM(sum),0)
FROM transactions
`)

	type Finance struct {
		Devices      int     `json:"devices"`
		Transactions int     `json:"transactions"`
		Revenue      float64 `json:"revenue"`
	}

	var f Finance

	row.Scan(
		&f.Devices,
		&f.Transactions,
		&f.Revenue,
	)

	c.JSON(200, f)
}
func reportPayments(c *gin.Context) {
	rows, err := db.Query(`
SELECT account,sum,created
FROM payments
ORDER BY created DESC
LIMIT 50
`)
	if err != nil {
		c.JSON(500, err.Error())
		return
	}

	type Payment struct {
		Account int     `json:"account"`
		Sum     float64 `json:"sum"`
		Created string  `json:"created"`
	}

	var payments []Payment

	for rows.Next() {

		var p Payment

		rows.Scan(
			&p.Account,
			&p.Sum,
			&p.Created,
		)
		payments = append(payments, p)
	}
	c.JSON(200, payments)
}
func reportTransactions(c *gin.Context) {
	rows, err := db.Query(`
SELECT account,sum,created_at
FROM transactions
ORDER BY created_at DESC
LIMIT 50
`)
	if err != nil {
		c.JSON(500, err.Error())
		return
	}
	type Tx struct {
		Account int     `json:"account"`
		Sum     float64 `json:"sum"`
		Created string  `json:"created"`
	}
	var data []Tx
	for rows.Next() {
		var t Tx
		rows.Scan(
			&t.Account,
			&t.Sum,
			&t.Created,
		)

		data = append(data, t)
	}
	c.JSON(200, data)
}
func reportStatus(c *gin.Context) {
	rows, err := db.Query(`
SELECT
account,
signal_wifi,
status,
data_status
FROM device_set
`)

	if err != nil {
		c.JSON(500, err.Error())
		return
	}

	type Status struct {
		Account  int    `json:"account"`
		Wifi     int    `json:"wifi"`
		Status   int    `json:"status"`
		LastSeen string `json:"last_seen"`
	}

	var result []Status

	for rows.Next() {

		var s Status

		rows.Scan(
			&s.Account,
			&s.Wifi,
			&s.Status,
			&s.LastSeen,
		)

		result = append(result, s)
	}

	c.JSON(200, result)
}
