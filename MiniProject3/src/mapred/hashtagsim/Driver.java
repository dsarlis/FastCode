package mapred.hashtagsim;

import java.io.IOException;
import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Driver {

	public static void main(String args[]) throws Exception {
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");
		String tmpdir = parser.get("tmpdir");

		getWordFeatureVector(input, tmpdir + "/feature_vector");
		getHashtagSimilarities(tmpdir + "/feature_vector", output);
	}

	/**
	 * Calculates the feature vector for all words
	 * @param input
	 * @param output
	 * @throws Exception
	 */
	private static void getWordFeatureVector(String input, String output)
			throws Exception {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Get feature vector for all words");
		job.setClasses(HashtagMapper.class, HashtagReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);
		job.run();
	}

	/**
	 * Calculates the similarities for all hashtag pairs
	 * @param input
	 * @param output
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void getHashtagSimilarities(String input, String output) throws IOException,
			ClassNotFoundException, InterruptedException {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Get similarities between pairs of hashtags");
		job.setClasses(SimilarityMapper.class, SimilarityReducer.class, SimilarityCombiner.class);
		job.setMapOutputClasses(Text.class, IntWritable.class);
		job.run();
	}

}
