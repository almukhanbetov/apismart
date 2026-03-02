package main

import (
	"database/sql"
	"log"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/go-sql-driver/mysql"
)

func mustEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("Missing env: %s", key)
	}
	return val
}

func main() {

	mysqlDSN := mustEnv("MYSQL_DSN")
	mqttBroker := mustEnv("MQTT_BROKER")
	mqttUser := mustEnv("MQTT_USER")
	mqttPass := mustEnv("MQTT_PASS")

	// MySQL
	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		log.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}
	log.Println("✅ MySQL Connected")

	// MQTT
	opts := mqtt.NewClientOptions()
	opts.AddBroker(mqttBroker)
	opts.SetUsername(mqttUser)
	opts.SetPassword(mqttPass)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}
	log.Println("✅ MQTT Connected")

	r := gin.Default()

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"mysql": "connected",
			"mqtt":  "connected",
			"time":  time.Now(),
		})
	})

	r.Run(":8080")
}