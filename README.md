## 广告集市 Spark 程序计算结果写入京东云  

### 技术调研
* 京东云 OSS 使用 Amazon S3 协议进行数据的上传、检索和管理。因此，在项目中引入 aws-java-sdk 依赖即引入了 Java 程序连接京东云的 API 层
* Spark 程序写入 S3 需要引入 hadoop-aws 依赖，由于广告集市上 hadoop project 的版本是 2.7.1，因此 hadoop-aws 也需要使用 2.7.1 版本，
进而导致 aws-java-sdk 需要引入 1.7.4 版本，这是因为 aws 对版本号非常敏感（一团糟）决定的。

### 2019.06.17 Spark on K8S 
spark version 2.4.3
hadoop version 2.7.3
aws-java-sdk 1.7.4

### 解决 spark 写入 s3 问题

