package com.jd.ads.n9jdcloud;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class JDCloudSparkWriter {
    private String s3Protocol = "s3a://";
    private String s3Bucket = "ads-model-user1";
    private String s3Path = "/kleguan";
    private SparkSession sparkSession(){
        return SparkSession.builder().enableHiveSupport()
                .config("spark.speculation", "true")
                .config("spark.speculation.quantile", 0.95)
                .config("hive.exec.orc.splits.strategy", "BI")
                .config("hive.exec.dynamic.partition", "true")
                .config("hive.exec.dynamic.partition.mode", "nonstrict")
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                .config("fs.s3a.awsAccessKeyId", "54DF7BD5FEBC058BA92CAEAD7562762F")
                .config("fs.s3a.awsSecretAccessKey", "FA6244141F88CC479BE59A6531E3BB0D")
                .getOrCreate();
    }

    private Dataset<Row> sampleDataset(SparkSession spark){
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("id", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("col1", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("key1", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("key2", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("key3", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("key4", DataTypes.StringType, false));
        StructType schema = DataTypes.createStructType(fields);

        List<Row> rows = new ArrayList<>();
        Row row = RowFactory.create("aa", 1, "a1000", "a1001", "a1002","a1003");
        rows.add(row);
        row = RowFactory.create("bb", 2, "b1000", "b1001", "b1002","b1003");
        rows.add(row);
        row = RowFactory.create("cc", 1, "c1000", "c1001", "c1002","c1003");
        rows.add(row);
        row = RowFactory.create("aa", 2, "a2000", "a2001", "a2002","a2003");
        rows.add(row);
        row = RowFactory.create("bb", 1, "b2000", "b2001", "b2002","b2003");
        rows.add(row);
        row = RowFactory.create("cc", 1, "c2000", "c2001", "c2002","c2003");
        rows.add(row);
        Dataset<Row> df = spark.createDataFrame(rows, schema);

        return df;
    }

    public static void main(String[ ] args)  {
        JDCloudSparkWriter sparkWriter = new JDCloudSparkWriter();
        SparkSession spark = sparkWriter.sparkSession();

        /*Dataset<Row> df = spark.sql("select * from model.dmp_user_store_label_orc " +
                "where dt='2019-05-13'");*/
        Dataset<Row> df = sparkWriter.sampleDataset(spark);

        df.show();

        df.write().format("csv").option("header","true").mode("overwrite")
                .save(sparkWriter.s3Protocol + sparkWriter.s3Bucket + sparkWriter.s3Path);
        spark.stop();
    }
}
