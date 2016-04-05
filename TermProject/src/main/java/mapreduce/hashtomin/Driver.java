package mapreduce.hashtomin;

import mapreduce.job.Optimizedjob;
import mapreduce.util.SimpleParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class Driver {

    public static void main(String[] args) throws Exception {
        SimpleParser parser = new SimpleParser(args);
        String input = parser.get("input");
        String tmpdir = parser.get("tmpdir");

        createClusters(input, tmpdir + "/clusters");
        input = tmpdir + "/clusters";
        long counter = 1;
        int iteration = 0;
        while (counter > 0) {
            String output = parser.get("output") + iteration;
            counter = hashToMin(input, output);
            input = output;
            iteration++;
        }
    }

    private static void createClusters(String input, String output)
            throws Exception {
        Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
                "Create clusters job");
        job.setClasses(FirstStepMapper.class, FirstStepReducer.class, null);
        job.setMapOutputClasses(Text.class, Text.class);
        job.run(false);
    }

    private static long hashToMin(String input, String output)
            throws Exception {
        Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
                "Hash to min job");
        job.setClasses(SecondStepMapper.class, SecondStepReducer.class, null);
        job.setMapOutputClasses(Text.class, Text.class);
        long counter = job.run(true);
        return counter;
    }
}
