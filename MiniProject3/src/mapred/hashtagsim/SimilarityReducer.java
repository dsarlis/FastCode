package mapred.hashtagsim;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SimilarityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Context context)
			throws IOException, InterruptedException {		

		/* Sum up the partial similarity scores to get the full score */
		int similarity = 0;
		for (IntWritable v : value) {
			similarity += v.get();
		}
		
		context.write(key, new IntWritable(similarity));
	}

}
