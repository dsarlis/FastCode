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
			String hashName = hashtag.toString();
			Integer count = counts.get(hashName);

			if (count == null) {
				count = 0;
			}
			count++;
			counts.put(hashName, count);
		}
		
		/*
		 * We're serializing the hashtag cooccurrence count as a string of the following form:
		 * 
		 * hashtag1:count1;hashtag2:count2;...;hashtagN:countN;
		 */
		StringBuilder builder = new StringBuilder();
		for (Map.Entry<String, Integer> entry : counts.entrySet()) {
			/* Build the list of hashtag counts and output in each step
			* So the first key-value pair would be: {word, hashtag1:count1}
			* The second would be: {word, hashtag1:count1;hashtag2:count2}
			* The third would be: {word, hashtag1:count1;hashtag2:count2;hashtag3:count3}
			* ...
			*/
			builder.append(entry.getKey()).append(":").append(entry.getValue()).append(";");
			context.write(key, new Text(builder.toString()));
		}
	}
}
