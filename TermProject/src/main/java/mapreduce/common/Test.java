package mapreduce.common;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Test {

	public static void main(String[] args) {
        AmazonS3Client s3Client = new AmazonS3Client(new ProfileCredentialsProvider());

        for (int i = 0; i < 29; i++) {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName("18645-termproject-output")
                    .withPrefix("twophase-medium-largeStar-" + i);
            ObjectListing objectListing;

            int count = 0;
            do {
                objectListing = s3Client.listObjects(listObjectsRequest);
                for (S3ObjectSummary objectSummary :
                        objectListing.getObjectSummaries()) {
                    if (objectSummary.getKey().contains("part-r-00007")) {
                        System.out.println(objectSummary.getETag());
                        count++;
                    }
                }
                listObjectsRequest.setMarker(objectListing.getNextMarker());
            } while (objectListing.isTruncated());
//            System.out.println(count);
        }
    }
}
