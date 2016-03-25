package mapred.ngramcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mapred.util.Tokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NgramCountMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] tokens = Tokenizer.tokenize(line);
		int n = context.getConfiguration().getInt("n", 5);
		List<String> words = new ArrayList<String>();

		for (String token : tokens) {
			if (!token.equals("")){
				words.add(token);
			}
		}

		for (int i = 0; i < words.size(); i++) {
			StringBuilder builder = new StringBuilder();
			boolean first = true;

			for (int j = i; j < i + n && i + n <= words.size(); j++){
				if (first) {
					first = false;
				} else {
					builder.append(" ");
				}
				builder.append(words.get(j));
			}

			if (i + n <= words.size()) {
				context.write(new Text(builder.toString()), NullWritable.get());
			}
		}
	}

}
