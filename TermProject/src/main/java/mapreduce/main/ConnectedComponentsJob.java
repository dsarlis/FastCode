package mapreduce.main;

import mapreduce.hashtomin.FirstStepMapper;
import mapreduce.hashtomin.FirstStepReducer;
import mapreduce.hashtomin.SecondStepMapper;
import mapreduce.hashtomin.SecondStepReducer;
import mapreduce.job.Optimizedjob;
import mapreduce.util.SimpleParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class ConnectedComponentsJob {

    public static void main(String[] args) throws Exception {
        SimpleParser parser = new SimpleParser(args);
        String input = parser.get("input");
        String output = parser.get("output");
        String tmpdir = parser.get("tmpdir");

        createClusters(input, tmpdir + "/feature_vector");
        hashToMin(tmpdir + "/feature_vector", output);
    }

    private static void createClusters(String input, String output)
            throws Exception {
        Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
                "Create clusters job");
        job.setClasses(FirstStepMapper.class, FirstStepReducer.class, null);
        job.setMapOutputClasses(Text.class, Text.class);
        job.run();
    }

    private static void hashToMin(String input, String output)
            throws Exception {
        Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
                "Hash to min job");
        job.setClasses(SecondStepMapper.class, SecondStepReducer.class, null);
        job.setMapOutputClasses(Text.class, Text.class);
        job.run();
    }
}
