package main

import (
	"database/sql"
	"log"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {

	// ===============================
	// ENV VARIABLES
	// ===============================

	mysqlDSN := os.Getenv("MYSQL_DSN")
	mqttBroker := os.Getenv("MQTT_BROKER")
	mqttUser := os.Getenv("MQTT_USER")
	mqttPass := os.Getenv("MQTT_PASS")

	if mysqlDSN == "" {
		log.Fatal("MYSQL_DSN not set")
	}

	// ===============================
	// MYSQL
	// ===============================

	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		log.Fatal("MySQL open error:", err)
	}

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	if err := db.Ping(); err != nil {
		log.Fatal("MySQL ping error:", err)
	}

	log.Println("✅ MySQL Connected")

	// ===============================
	// MQTT
	// ===============================

	opts := mqtt.NewClientOptions()
	opts.AddBroker(mqttBroker)
	opts.SetUsername(mqttUser)
	opts.SetPassword(mqttPass)
	opts.SetConnectTimeout(5 * time.Second)

	client := mqtt.NewClient(opts)

	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		log.Fatal("MQTT connection error:", token.Error())
	}

	log.Println("✅ MQTT Connected")

	// ===============================
	// HTTP SERVER
	// ===============================

	r := gin.Default()

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"mysql": "connected",
			"mqtt":  "connected",
			"time":  time.Now(),
		})
	})

	log.Println("🚀 Server started on :8080")

	r.Run("0.0.0.0:8080")
}