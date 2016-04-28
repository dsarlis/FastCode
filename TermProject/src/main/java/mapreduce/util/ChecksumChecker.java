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

    /**
     * Method to check if output files have changed
     * @param path
     * @return
     */
    public boolean checkSumsChanged(String path) {
        Set<String> newCheckSums = new HashSet<>();
        AmazonS3Client s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
        /* Get the last output folder */
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(BUCKET_NAME)
                .withPrefix(path);
        ObjectListing objectListing;

        /* Go through the files and check if they are already present in the Set
         * If not, it means that the output has changed
         * The set holds a list of the checksums of the previous
         * iteration to test against the present iteration
         */
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

        /* If at least one file has changed, then it means we need to still keep going */
        if (changed) {
            checkSums = newCheckSums;
            return true;
        }
        return false;
    }
}