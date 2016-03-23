package mapred.hashtagsim;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HashtagReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {		
		Map<String, Integer> counts = new HashMap<String, Integer>();

		for (Text hashtag : value) {
			String w = hashtag.toString();
			String[] tokens = w.split(";");
			Integer count = counts.get(tokens[0]);

			if (count == null) {
				count = 0;
			}
			count += Integer.parseInt(tokens[1]);
			counts.put(tokens[0], count);
		}
		
		/*
		 * We're serializing the word cooccurrence count as a string of the following form:
		 * 
		 * hashtag1:count1;hashtag2:count2;...;hashtagN:countN;
		 */
		StringBuilder builder = new StringBuilder();
		for (Map.Entry<String, Integer> entry : counts.entrySet())
			builder.append(entry.getKey()).append(":").append(entry.getValue()).append(";");
		
		context.write(key, new Text(builder.toString()));
	}
}