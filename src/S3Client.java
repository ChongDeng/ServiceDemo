import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;

import java.io.*;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

public class S3Client {

    private static String bucket_name = "mobile-analytics-dc-10124019";

    static AmazonS3 s3Clientt = null;

    static String AccessKey = "";
    static String SecretKey = "";


    static{

        getCredential();

        BasicAWSCredentials credentials = new BasicAWSCredentials(AccessKey, SecretKey);
        AmazonS3 s3Clientt = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(Regions.US_WEST_1)
                .build();

    }

    private static void getCredential(){
        System.out.println("path: " + System.getProperty("user.dir") + "\\config\\credential.txt");
        File file = new File(System.getProperty("user.dir") + "\\config\\credential.txt");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            if( (tempString = reader.readLine()) != null){
                AccessKey = tempString;
            }
            if( (tempString = reader.readLine()) != null){
                SecretKey = tempString;
            }
            reader.close();
        } catch (Exception ex) {
           System.out.print("exception: " + ex);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    System.out.print("cant close file: " + e1);
                }
            }
        }
    }

    public static Bucket getBucket(String bucket_name) {

        AmazonS3 s3Client = getS3Client();

        Bucket named_bucket = null;
        List<Bucket> buckets = s3Client.listBuckets();
        for (Bucket b : buckets) {
            System.out.println("bucket name: " + b.getName());
            if (b.getName().equals(bucket_name)) {
                named_bucket = b;
            }
        }
        return named_bucket;
    }

    private static AmazonS3 getS3Client(){
        BasicAWSCredentials credentials = new BasicAWSCredentials(AccessKey, SecretKey);

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(Regions.US_WEST_1)
                .build();
        return s3Client;
    }

    private static void CreateBucket(String BucketName){
        Bucket b = null;

        AmazonS3 s3Client = getS3Client();


        if (s3Client.doesBucketExist(BucketName)) {
            System.out.format("Bucket %s already exists.\n", BucketName);
            b = getBucket(BucketName);
        } else {
            try {
                b = s3Client.createBucket(BucketName);
            } catch (AmazonS3Exception e) {
                System.err.println(e.getErrorMessage());
            }
        }
    }

    private static void DeleteBucket(String BucketName){

        AmazonS3 s3Client = getS3Client();

        try {

            //step1: remove objects
            System.out.println(" - removing objects from bucket");
            ObjectListing object_listing = s3Client.listObjects(BucketName);
            while (true) {
                for (Iterator<?> iterator =
                     object_listing.getObjectSummaries().iterator();
                     iterator.hasNext();) {
                    S3ObjectSummary summary = (S3ObjectSummary)iterator.next();
                    s3Client.deleteObject(BucketName, summary.getKey());
                }

                // more object_listing to retrieve?
                if (object_listing.isTruncated()) {
                    object_listing = s3Client.listNextBatchOfObjects(object_listing);
                } else {
                    break;
                }
            };

            //step2: delete bucket
            System.out.println(" OK, bucket ready to delete!");
            s3Client.deleteBucket(BucketName);

        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }


    private static boolean UploadSingleObject(String BucketName, String ObjectPath){

        AmazonS3 s3Client = getS3Client();

        String ObjectKeyName = Paths.get(ObjectPath).getFileName().toString();

        long FileLen = Paths.get(ObjectPath).toFile().length();
        System.out.format("Uploading %s to S3 bucket %s...\n", ObjectPath, BucketName);
        try {
            //s3Client.putObject(BucketName, ObjectKeyName, ObjectPath);
            File file = new File(ObjectPath);
            s3Client.putObject(new PutObjectRequest(
                    BucketName, ObjectKeyName, file));
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            return false;
        }

        return true;
    }


    private static boolean UploadSingleObjectWithSpecifiedName(String BucketName, String ObjectPath, String KeyName){

        AmazonS3 s3Client = getS3Client();

        String ObjectKeyName = KeyName;

        long FileLen = Paths.get(ObjectPath).toFile().length();
        System.out.format("Uploading %s to S3 bucket %s...\n", ObjectPath, BucketName);
        try {
            //s3Client.putObject(BucketName, ObjectKeyName, ObjectPath);
            File file = new File(ObjectPath);
            s3Client.putObject(new PutObjectRequest(
                    BucketName, ObjectKeyName, file));
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            return false;
        }

        return true;
    }

    public static void main( String[] args ) {
        System.out.println( "begin to run!" );

//        CreateBucket("mobile-analytics-dc-10124024");
//        DeleteBucket("mobile-analytics-dc-10124024");
//        DeleteBucket("mobile-analytics-dc-10124023");
//        DeleteBucket("mobile-analytics-dc-10124022");
//        DeleteBucket("mobile-analytics-dc-10124020");
//        DeleteBucket("mobile-analytics-dc-10124019");


        String BucketName = "mobile-analytics-dc-10124024";
        //String ObjectPath = "C:\\dc_test\\yuanyuan.png";
        String ObjectPath = "C:\\Users\\fqyya\\Desktop\\qq.jpg";
        //String ObjectPath = "C:\\dc_exercise\\qian3.jpg";


        //if(UploadSingleObject(BucketName, ObjectPath)){
        if(UploadSingleObjectWithSpecifiedName(BucketName, ObjectPath, "hello kitty")){
            System.out.println("successfully upload object " + ObjectPath);
        }
        else{
            System.err.println("Failed to upload object " + ObjectPath);
        }


    }
}
