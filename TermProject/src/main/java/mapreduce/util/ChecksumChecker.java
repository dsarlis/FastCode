package mapreduce.util;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.HashSet;
import java.util.Set;

public class ChecksumChecker {
    private static final String BUCKET_NAME = "18645-termproject-output";
    private Set<String> checkSums;

    public ChecksumChecker() {
        checkSums = new HashSet<>();
    }

    public boolean checkSumsChanged(String path) {
        Set<String> newCheckSums = new HashSet<>();
        AmazonS3Client s3Client = new AmazonS3Client();
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(BUCKET_NAME)
                .withPrefix(path);
        ObjectListing objectListing;

        boolean changed = false;
        do {
            objectListing = s3Client.listObjects(listObjectsRequest);
            for (S3ObjectSummary objectSummary :
                    objectListing.getObjectSummaries()) {
                if (objectSummary.getKey().contains("part")) {
                    String checkSum = objectSummary.getETag().toString();
                    if (!checkSums.contains(checkSum)) {
                        changed = true;
                    }
                    newCheckSums.add(checkSum);
                }
            }
            listObjectsRequest.setMarker(objectListing.getNextMarker());
        } while (objectListing.isTruncated());

        if (changed) {
            checkSums = newCheckSums;
            return true;
        } else {
            return false;
        }
    }
}
