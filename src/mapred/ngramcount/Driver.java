package mapred.ngramcount;

import java.io.IOException;
import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class Driver {

	public static void main(String args[]) throws Exception {
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");
		int n = Integer.parseInt(parser.get("n"));

		getJobFeatureVector(input, output, n);
	}

	private static void getJobFeatureVector(String input, String output, int n)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		configuration.setInt("n", n);
		Optimizedjob job = new Optimizedjob(configuration, input, output,
				"Compute NGram Count");

		job.setClasses(NgramCountMapper.class, NgramCountReducer.class, null);
		job.setMapOutputClasses(Text.class, NullWritable.class);

		job.run();
	}	
}
