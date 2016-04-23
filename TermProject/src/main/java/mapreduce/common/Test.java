package mapreduce.common;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Test {

	public static void main(String[] args) {
        AmazonS3Client s3Client = new AmazonS3Client();
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName("18645-termproject-output")
                .withPrefix("hashtomin-big0");
        ObjectListing objectListing;

        do {
            objectListing = s3Client.listObjects(listObjectsRequest);
            for (S3ObjectSummary objectSummary :
                    objectListing.getObjectSummaries()) {
                if (objectSummary.getKey().contains("part")) {
                    System.out.println(objectSummary.getETag());
                }
            }
            listObjectsRequest.setMarker(objectListing.getNextMarker());
        } while (objectListing.isTruncated());
	}
}
