### 前置条件
下载 mysql 数据库
运行 mysql 数据 容器作为数据源
```
git clone https://gitee.com/bigriver/test_db.git
docker run -d -e MYSQL_ALLOW_EMPTY_PASSWORD=true -v $(pwd)/test_db:/test_db -p 3306:3306 mysql:5.7
```

运行 elastic 容器
```
docker run -d --name elasticsearch -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" elasticsearch:7.17.6
```
### 导入数据
导入 mysql & 验证

```
docker exec <containerid> bash -c 'cd test_db && mysql < employees.sql'
docker exec <containerid> bash -c 'cd test_db && mysql -t < test_employees_md5.sql'
```

~~ 导入 mysql 数据到 elastic ~~

~~ 运行 pkg/initElasticData/main.go ~~

~~ go run pkg/initElasticData/main.go ~~


### 接入 kafka
运行 kafka 服务（依赖 zookeeper)
在 kafka 目录下运行
```
docker-compose up -d
```

启动生产者
在 pkg/kafkaStream 路径下
```
go run main.go
```

启动消费者
在 pkg/initElasticData 路径下
```
go run main.go
```

go run main.go

### 开启搜索服务
然后运行 项目跟路径下的 main.go 开启 httpserver 服务 通过 url 验证搜索
go run main.go
