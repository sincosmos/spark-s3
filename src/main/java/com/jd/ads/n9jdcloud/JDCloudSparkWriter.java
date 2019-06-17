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
import java.util.ResourceBundle;

public class JDCloudSparkWriter {
    private static ResourceBundle resource = ResourceBundle.getBundle("config");

    private SparkSession sparkSession(){
        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key",resource.getString("fs.s3a.access.key"));
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key",resource.getString("fs.s3a.secret.key"));
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.impl",resource.getString("fs.s3a.impl"));
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint",resource.getString("fs.s3a.endpoint.external"));
        return spark;
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

        //Dataset<Row> df = spark.sql("select * from model.feature_20190507_d00420fa718b11e9a047fa163e83229f");
        Dataset<Row> df = sparkWriter.sampleDataset(spark);

        //df.show();
        String path = resource.getString("cloud.protocol")
                + resource.getString("cloud.bucket") + "test" + System.currentTimeMillis();
        System.out.println(path);
        long t1 = System.currentTimeMillis();
        df.write().format("csv").option("header","true").mode("overwrite")
                .save(path);
        long t2 = System.currentTimeMillis();
        System.out.println("time consuming: " + (t2-t1)/1000 + "s");
        spark.stop();
    }
}
