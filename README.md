## 广告集市 Spark 程序计算结果写入京东云  

### 技术调研
* 京东云 OSS 使用 Amazon S3 协议进行数据的上传、检索和管理。因此，在项目中引入 aws-java-sdk-s3 依赖即引入了 Java 程序连接京东云的 API 层
* Spark 程序写入 S3 需要引入 hadoop-aws 依赖 ???