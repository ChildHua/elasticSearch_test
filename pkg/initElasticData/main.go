package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"testElastic/pkg/kafkaStream/common"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var (
	db  *gorm.DB
	es  *elasticsearch.Client
	err error

	testindex   string
	indexName   string
	deptInfo    map[string]string
	deptMngInfo map[string]interface{}
)

const (
	kafkaIndex = "employees_kafka"
)

func init() {
	// refer https://github.com/go-sql-driver/mysql#dsn-data-source-name for details
	dsn := "root:@tcp(127.0.0.1:3306)/employees?charset=utf8mb4&parseTime=True&loc=Local"
	db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	es, err = elasticsearch.NewDefaultClient()
	if err != nil {
		panic(err)
	}

	// testindex = "my-index-000001"
	testindex = "employees"
	indexName = "employees"

	generateDeptNameWithNo()
	generateDeptMngWithNo()
}

func main() {
	// CreateIndex()
	// run()
	kafkaStream()
	select {}
}

func run() {
	var results []map[string]interface{}
	var result map[string]interface{}
	rows, err := db.Table("employees").Rows()
	if err != nil {
		panic(err)
	}

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         indexName,       // The default index name
		Client:        es,              // The Elasticsearch client
		NumWorkers:    2,               // The number of worker goroutines
		FlushBytes:    int(5e+6),       // The flush threshold in bytes
		FlushInterval: 5 * time.Second, // The periodic flush interval
	})
	if err != nil {
		log.Fatalf("Error creating the indexer: %s", err)
		return
	}

	start := time.Now().UTC()

	i := 1
	for rows.Next() {
		err := db.ScanRows(rows, &result)
		if err != nil {
			log.Fatal(err)
			return
		}
		emp_no := fmt.Sprintf("%v", result["emp_no"])
		fmt.Println("current emp_no", emp_no)
		results = nil
		db.Table("dept_emp").Where("emp_no=?", result["emp_no"]).Find(&results)
		// 部门名称拼接
		for _, dept := range results {
			dept_no := fmt.Sprintf("%v", dept["dept_no"])
			dept["dept_name"] = deptInfo[dept_no]
		}
		result["department"] = results

		results = nil
		db.Table("titles").Where("emp_no=?", result["emp_no"]).Find(&results)
		result["title"] = results

		results = nil
		db.Table("salaries").Where("emp_no=?", result["emp_no"]).Find(&results)
		result["salaries"] = results

		// 部门经理信息
		result["dept_mng"] = deptMngInfo[emp_no]

		CreateBatchDoc(bi, i, result)
		i++
	}

	// Close the indexer
	//
	if err := bi.Close(context.Background()); err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}
	biStats := bi.Stats()

	// Report the results: number of indexed docs, number of errors, duration, indexing rate
	//
	log.Println(strings.Repeat("▔", 65))

	dur := time.Since(start)

	if biStats.NumFailed > 0 {
		log.Fatalf(
			"Indexed [%s] documents with [%s] errors in %s (%s docs/sec)",
			humanize.Comma(int64(biStats.NumFlushed)),
			humanize.Comma(int64(biStats.NumFailed)),
			dur.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed))),
		)
	} else {
		log.Printf(
			"Sucessfuly indexed [%s] documents in %s (%s docs/sec)",
			humanize.Comma(int64(biStats.NumFlushed)),
			dur.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed))),
		)
	}
}

func CreateIndex() {
	res, err := es.Indices.Create(testindex, es.Indices.Create.WithBody(strings.NewReader(`{
		"mappings": {
			"properties": {
				"salaries": {
					"type": "nested"
				},
				"title":{
					"type": "nested"
				},
				"department":{
					"type":"nested"
				},
				"dept_mng":{
					"type":"nested"
				}
			}
		}
	  }`)))
	if err != nil {
		panic(err)
	}
	fmt.Println(res)
}

func CreateIndexForKafka() {
	// Re-create the index
	if _, err = es.Indices.Delete([]string{kafkaIndex}); err != nil {
		log.Fatalf("Cannot delete index: %s", err)
	}
	res, err := es.Indices.Create(kafkaIndex, es.Indices.Create.WithBody(strings.NewReader(`{
		"mappings": {
			"properties": {
				"salaries": {
					"type": "nested"
				},
				"title":{
					"type": "nested"
				},
				"department":{
					"type":"nested"
				},
				"dept_mng":{
					"type":"nested"
				}
			}
		}
	  }`)))
	if err != nil {
		panic(err)
	}
	fmt.Println(res)
}

func CreateBatchDoc(bi esutil.BulkIndexer, docid int, source map[string]interface{}) {
	data, err := json.Marshal(source)
	if err != nil {
		panic(err)
	}
	err = bi.Add(
		context.Background(),
		esutil.BulkIndexerItem{
			// Action field configures the operation to perform (index, create, delete, update)
			Action: "index",

			// DocumentID is the (optional) document ID
			DocumentID: strconv.Itoa(docid),

			// Body is an `io.Reader` with the payload
			Body: bytes.NewReader(data),

			// OnSuccess is called for each successful operation
			OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
				// atomic.AddUint64(&countSuccessful, 1)
			},

			// OnFailure is called for each failed operation
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				if err != nil {
					log.Printf("ERROR: %s", err)
				} else {
					log.Printf("ERROR: %s: %s", res.Error.Type, res.Error.Reason)
				}
			},
		},
	)
	if err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}
}

func generateDeptNameWithNo() {
	type dept struct {
		DeptNo   string
		DeptName string
	}
	var result dept
	// var results []map[string]interface{}
	rows, err := db.Table("departments").Rows()
	if err != nil {
		panic(err)
	}
	deptInfo = make(map[string]string)
	for rows.Next() {
		db.ScanRows(rows, &result)
		deptInfo[result.DeptNo] = result.DeptName
	}
}

func generateDeptMngWithNo() {
	type dept struct {
		EmpNo    string
		DeptNo   string
		FromDate string
		ToData   string
		DeptName string
	}
	var result dept
	// var results []map[string]interface{}
	rows, err := db.Table("dept_manager").Rows()
	if err != nil {
		panic(err)
	}
	deptMngInfo = make(map[string]interface{})
	for rows.Next() {
		db.ScanRows(rows, &result)
		result.DeptName = deptInfo[result.DeptNo]
		deptMngInfo[result.EmpNo] = result
	}
}

func kafkaStream() {
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         kafkaIndex,      // The default index name
		Client:        es,              // The Elasticsearch client
		NumWorkers:    2,               // The number of worker goroutines
		FlushBytes:    int(5e+6),       // The flush threshold in bytes
		FlushInterval: 5 * time.Second, // The periodic flush interval
	})
	if err != nil {
		log.Fatalf("Error creating the indexer: %s", err)
		return
	}

	go empConsm(bi)
	go salaryConsm(bi)
}

func empConsm(bi esutil.BulkIndexer) {
	consm := common.NewConsumer(common.Group, []string{common.Topic_emp}, &common.EventHandler{
		Claim: func(data common.KafkaMsg) error {
			i, _ := strconv.Atoi(data.ID)
			fmt.Println("emp_msg id:", i)
			CreateBatchDoc(bi, i, data.Detail)
			return nil
		},
	})
	defer consm.Stop()
	consm.Consume() // 异步消费
}

func salaryConsm(bi esutil.BulkIndexer) {
	empsalary_avg := make(map[float64][]int)
	consm := common.NewConsumer(common.Group, []string{common.Topic_salary}, &common.EventHandler{
		Claim: func(data common.KafkaMsg) error {
			i, _ := strconv.Atoi(data.ID)
			emp_id := data.Detail["emp_no"].(float64)
			salary := data.Detail["salary"].(float64)
			if v, ok := empsalary_avg[emp_id]; ok {
				count := v[0]
				avg := v[1]
				v[1] = (avg*count + int(salary)) / (count + 1)
				v[0] = count + 1
			} else {
				s := []int{1, int(salary)}
				empsalary_avg[emp_id] = s
			}
			fmt.Println("employee avg :", empsalary_avg)
			CreateBatchDoc(bi, i, data.Detail)
			return nil
		},
	})
	defer consm.Stop()
	consm.Consume() // 异步消费
}
