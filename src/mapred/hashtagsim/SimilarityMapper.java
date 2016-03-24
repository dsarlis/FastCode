package mapred.hashtagsim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimilarityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] wordFeatureVector = line.split("\\s+", 2);

		List<HashtagCount> features = parseFeatureVector(wordFeatureVector[1]);

        /* Get the last element and traverse in reverse order to get the pairs
         * The next key-value read from the mapper will generate the rest of the
         * pairs from second to last element till the first one
         */
		HashtagCount firstHashtagCount = features.get(features.size()-1);
		for (int j = features.size()-2; j >= 0; j--) {
			HashtagCount secondHashtagCount = features.get(j);
			String firstHashName = firstHashtagCount.getHashtag();
			String secondHashName = secondHashtagCount.getHashtag();
			String outKey;

            /* Create the pair in the same order, regardless of the order encountered */
			if (secondHashName.compareTo(firstHashName) < 0) {
				outKey = secondHashName + " " + firstHashName;
			} else {
				outKey = firstHashName + " " + secondHashName;
			}

            /* Output the key and the partial similarity score for the hashtag pair
             * considering the contribution of the input key (word)
             */
			context.write(new Text(outKey),
					new IntWritable(firstHashtagCount.getCount() * secondHashtagCount.getCount()));
		}
	}

	/**
	 * De-serialize the feature vector into a map
	 * 
	 * @param featureVector
	 *            The format is "hashtag1:count1;hashtag2:count2;...;hashtagN:countN;"
	 * @return A HashMap, with key being each hashtag and value being the count.
	 */
	private List<HashtagCount> parseFeatureVector(String featureVector) {
		List<HashtagCount> result = new ArrayList<HashtagCount>();
		String[] features = featureVector.split(";");
		for (String feature : features) {
			String[] word_count = feature.split(":");
			result.add(new HashtagCount(word_count[0], Integer.parseInt(word_count[1])));
		}
		return result;
	}

	private class HashtagCount {

		private String hashtag;
		private int count;

		public HashtagCount(String hashtag, int count) {
			this.hashtag = hashtag;
			this.count = count;
		}

		public String getHashtag() {
			return hashtag;
		}

		public int getCount() {
			return count;
		}

	}
}














