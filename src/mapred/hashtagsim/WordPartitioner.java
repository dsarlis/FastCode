package mapred.hashtagsim;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class WordPartitioner extends Partitioner<Text, Text> {

    /**
     *
     * @param key
     * @param value
     * @param numReduceTasks
     * @return
     */
    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        String line = value.toString();
        String[] wordFeatureVector = line.split("\\s+", 2);

        String[] features = wordFeatureVector[1].split(";");

        return features.length % numReduceTasks;
    }
}
