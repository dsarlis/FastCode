package mapreduce.main;

import mapreduce.common.FirstStepMapper;
import mapreduce.common.FirstStepReducer;
import mapreduce.common.MergeReducer;
import mapreduce.job.OptimizedJob;
import mapreduce.util.SimpleParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import java.io.IOException;


public class HashToDriver {

    public static void main(String[] args, Class c) throws Exception {
        SimpleParser parser = new SimpleParser(args);
        String input = parser.get("input");
        String tmpDir = parser.get("tmpdir");

        createClusters(input, tmpDir + "/clusters");
        input = tmpDir + "/clusters";
        long counter = 1;
        int iteration = 0;

        while (counter > 0) {
            String output = parser.get("output") + iteration;
            counter = hashStep(input, output, c);
            input = output;
            iteration++;
        }
    }

    private static void createClusters(String input, String output) throws Exception {
        OptimizedJob job = new OptimizedJob(new Configuration(), input, output, "Create clusters job");

        job.setClasses(FirstStepMapper.class, FirstStepReducer.class, null);
        job.setMapOutputClasses(Text.class, Text.class);
        job.run(false);
    }

    private static long hashStep(String input, String output, Class c)
            throws InterruptedException, IOException, ClassNotFoundException {
        OptimizedJob job = new OptimizedJob(new Configuration(), input, output,
                String.format("%s job", c.getSimpleName()));
        job.setClasses(c, MergeReducer.class, null);
        job.setMapOutputClasses(Text.class, Text.class);

        return job.run(true);
    }
}
