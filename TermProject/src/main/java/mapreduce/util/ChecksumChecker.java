package mapreduce.util;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import java.util.HashSet;
import java.util.Set;

public class ChecksumChecker {
    private static final String BUCKET_NAME = "18645-termproject-output";
    private Set<String> checkSums;

    public ChecksumChecker() {
        checkSums = new HashSet<>();
    }

    public boolean checkSumsChanged(String path) {
//        System.out.println("Inside checkSumsChanged");
        Set<String> newCheckSums = new HashSet<>();
        AmazonS3Client s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(BUCKET_NAME)
                .withPrefix(path);
        ObjectListing objectListing;

        boolean changed = false;
        do {
            objectListing = s3Client.listObjects(listObjectsRequest);
            for (S3ObjectSummary objectSummary :
                    objectListing.getObjectSummaries()) {
//                System.out.println("File: " + objectSummary.getKey());
                if (objectSummary.getKey().contains("part")) {
                    String checkSum = objectSummary.getETag().toString();
                    if (!checkSums.contains(checkSum)) {
//                        System.out.println("Found a file that has changed");
                        changed = true;
                    }
                    newCheckSums.add(checkSum);
                }
            }
            listObjectsRequest.setMarker(objectListing.getNextMarker());
        } while (objectListing.isTruncated());

//        System.out.println("Result for output: " + path + " hasChanged:" + changed);

        if (changed) {
            checkSums = newCheckSums;
            return true;
        }
        return false;
    }
}