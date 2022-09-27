package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"testElastic/pkg/kafkaStream/common"

	"github.com/Shopify/sarama"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var (
	db  *gorm.DB
	err error
)

func init() {
	// refer https://github.com/go-sql-driver/mysql#dsn-data-source-name for details
	dsn := "root:@tcp(127.0.0.1:3306)/employees?charset=utf8mb4&parseTime=True&loc=Local"
	db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}
}

func main() {

	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	empProducter()
	salaryProducter()
	select {}
}

func empProducter() {
	var result map[string]interface{}
	pro := common.NewEventProducer(common.Topic_emp)
	rows, err := db.Table(common.Topic_emp).Rows()
	if err != nil {
		panic(err)
	}
	i := 1
	for rows.Next() {
		err := db.ScanRows(rows, &result)
		if err != nil {
			log.Fatal(err)
			return
		}
		emp_no := fmt.Sprintf("%v", result["emp_no"])
		// 发送 kafka
		msg := &common.KafkaMsg{
			ID:     emp_no,
			Detail: result,
		}
		pro.Producer(msg)
		i++
	}
}

func salaryProducter() {
	var result map[string]interface{}
	pro := common.NewEventProducer(common.Topic_salary)
	rows, err := db.Table(common.Topic_salary).Rows()
	if err != nil {
		panic(err)
	}
	i := 40000
	for rows.Next() {
		err := db.ScanRows(rows, &result)
		if err != nil {
			log.Fatal(err)
			return
		}
		// 发送 kafka
		msg := &common.KafkaMsg{
			ID:     strconv.Itoa(i),
			Detail: result,
		}
		pro.Producer(msg)
		i++
	}
}
