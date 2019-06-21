package com.jd.ads.n9jdcloud;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ResourceBundle;

public class JDCloudJavaRWLowerVersionSdk {
    private static final ResourceBundle resources = ResourceBundle.getBundle("config");

    public static void main(String[] args){
        JDCloudJavaRWLowerVersionSdk s3Rw = new JDCloudJavaRWLowerVersionSdk();
        AmazonS3 s3 = s3Rw.amazonS3();
        s3Rw.listS3Object(s3);
        s3Rw.s3Upload(s3, "{'task_id':'abcdef'}");
        s3Rw.listS3Object(s3);
        s3Rw.s3Download(s3, "test.json");
    }

    public AmazonS3 amazonS3(){
        final String accessKey = resources.getString("fs.s3a.access.key");
        final String secretKey = resources.getString("fs.s3a.secret.key");
        final String endpoint = resources.getString("fs.s3a.endpoint.external");
        ClientConfiguration config = new ClientConfiguration();
        AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey,secretKey);
        AWSCredentialsProvider awsCredentialsProvider = new StaticCredentialsProvider(awsCredentials);

        AmazonS3 s3 = new AmazonS3Client(awsCredentialsProvider, config);
        s3.setEndpoint(endpoint);

        return s3;
    }

    public void listS3Object(AmazonS3 s3){
        final String bucket = resources.getString("cloud.bucket");
        ObjectListing result = s3.listObjects(bucket);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        for (S3ObjectSummary os: objects) {
            System.out.println("* " + os.getKey());
        }
    }

    public void s3Upload(AmazonS3 s3, String json){
        final String bucket = resources.getString("cloud.bucket");
        InputStream in = null;
        try {
            in = new DataInputStream(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentType("application/json");
            objectMetadata.setContentLength(json.length());

            s3.putObject(bucket, "test.json", in, objectMetadata);
            System.out.format("Uploading %s to OSS bucket %s...\n", "test.json", bucket);
        }finally {
            if(in != null){
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void s3Download(AmazonS3 s3, String name){
        final String bucket = resources.getString("cloud.bucket");
        try {
            S3Object o = s3.getObject(bucket, name);
            S3ObjectInputStream s3is = o.getObjectContent();

            ByteArrayOutputStream content = new ByteArrayOutputStream();
            byte[] readBuf = new byte[1024];
            while (s3is.read(readBuf)> 0) {
                content.write(readBuf);
            }
            System.out.println("content:\n" + content.toString().trim());
            s3is.close();
        } catch (AmazonServiceException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        System.out.println("Done!");

    }
}
