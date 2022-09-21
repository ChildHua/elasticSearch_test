package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

var (
	es  *elasticsearch.Client
	err error

	indexName string
)

func init() {
	es, err = elasticsearch.NewDefaultClient()
	if err != nil {
		panic(err)
	}

	indexName = "employees3"
}

func main() {
	http.HandleFunc("/_search", Search)

	http.ListenAndServe(":80", nil)
}

func generateRootSearch(key, value string) interface{} {
	return map[string]map[string]string{
		"match": {
			key: value,
		},
	}
}

func generateNestedSearch(path string, fieldValues [][2]string) interface{} {
	match := map[string]string{}
	for _, v := range fieldValues {
		match[path+"."+v[0]] = v[1]
	}
	res := map[string]interface{}{
		"nested": map[string]interface{}{
			"path": path,
			"query": map[string]interface{}{
				"bool": map[string]interface{}{
					"must": []interface{}{
						map[string]interface{}{
							"match": match,
						},
					},
				},
			},
		},
	}
	return res
}

func empNameSearch(name string) interface{} {
	// res := make([]interface{}, 2)
	res := map[string]interface{}{
		"bool": map[string]interface{}{
			"should": []interface{}{
				map[string]interface{}{
					"bool": map[string]interface{}{
						"must": []interface{}{
							map[string]interface{}{"match": map[string]string{"first_name": name}},
						},
					},
				},
				map[string]interface{}{
					"bool": map[string]interface{}{
						"must": []interface{}{
							map[string]interface{}{"match": map[string]string{"last_name": name}},
						},
					},
				},
			},
		},
	}
	return res
}

func salariesSearch(f, t string) interface{} {
	res := map[string]interface{}{
		"nested": map[string]interface{}{
			"path": "salaries",
			"query": map[string]interface{}{
				"bool": map[string]interface{}{
					"filter": map[string]interface{}{
						"range": map[string]interface{}{
							"salaries.salary": map[string]string{
								"gte": f,
								"lt":  t,
							},
						},
					},
				},
			},
		},
	}
	return res
}

// "group_by_comments_date": {
// 	"date_histogram": {
// 	  "field": "comments.date",
// 	  "interval": "month",
// 	  "format": "yyyy-MM"
// 	},
func salariesAggs() interface{} {
	res := map[string]interface{}{
		"aggs": map[string]interface{}{
			"salaries": map[string]interface{}{
				"nested": map[string]string{
					"path": "salaries",
				},
				"aggs": map[string]interface{}{
					"by_month": map[string]interface{}{
						"date_histogram": map[string]string{
							"field":    "salaries.from_date",
							"interval": "month",
							"format":   "yyyy-MM",
						},
						"aggs": map[string]interface{}{
							"avg_salary": map[string]interface{}{
								"avg": map[string]string{
									"field": "salaries.salary",
								},
							},
						},
					},
					"avg_salaries": map[string]interface{}{
						"avg": map[string]string{
							"field": "salaries.salary",
						},
					},
				},
			},
		},
	}
	return res["aggs"]
}

func Search(w http.ResponseWriter, req *http.Request) {
	emp_name_key := "emp_name"
	dept_name_key := "dept_name"
	title_name_key := "title_name"
	salaries_from_key := "saraly_frome"
	salaries_to_key := "saraly_to"

	must := make([]interface{}, 0)

	q := req.URL.Query()
	emp_name := q.Get(emp_name_key)
	fmt.Println(emp_name)
	if emp_name != "" {
		must = append(must, empNameSearch(emp_name))
	}

	// dept
	dept_name := q.Get(dept_name_key)
	fmt.Println(dept_name)
	if dept_name != "" {
		must = append(must,
			generateNestedSearch("department", [][2]string{{dept_name_key, dept_name}}),
		)
	}
	// title
	title_name := q.Get(title_name_key)
	fmt.Println(title_name)
	if title_name != "" {
		must = append(must,
			generateNestedSearch("title", [][2]string{{title_name_key, title_name}}),
		)
	}

	// salaries
	salaries_from := q.Get(salaries_from_key)
	salaries_to := q.Get(salaries_to_key)
	fmt.Println("saraly range:", salaries_from, "-", salaries_to)
	if salaries_from != "" && salaries_to != "" {
		must = append(must,
			salariesSearch(salaries_from, salaries_to),
		)
	}

	reqMap := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": must,
			},
		},
		"aggs": salariesAggs(),
		"highlight": map[string]interface{}{
			"fields": map[string]interface{}{
				"first_name": map[string]string{
					"force_source": "true",
				},
			},
		},
	}
	d, err := json.Marshal(reqMap)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(d))
	esreq := esapi.SearchRequest{
		Index:  []string{indexName},
		Body:   bytes.NewReader(d),
		Human:  true,
		Pretty: true,
	}
	r, err2 := esreq.Do(context.Background(), es)
	if err2 != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(r)
}
