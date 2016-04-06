package mapreduce.hashtomin;

import mapreduce.job.OptimizedJob;
import mapreduce.main.HashStrategy;
import mapreduce.util.SimpleParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;


public class Driver {

    public static void main(String[] args) throws Exception {
        SimpleParser parser = new SimpleParser(args);
        String input = parser.get("input");
        String tmpDir = parser.get("tmpdir");
        String program = parser.get("program");
        HashStrategy hashStrategy = null;

        if (program.equals("hashtoall")) {
            hashStrategy = new HashStrategy<HashToAllMapper>(HashToAllMapper.class);
        } else if (program.equals("hashtoall")) {
            hashStrategy = new HashStrategy<HashToMinMapper>(HashToMinMapper.class);
        } else if (program.equals("hashtoall")) {
            hashStrategy = new HashStrategy<HashGreaterToMinMapper>(HashGreaterToMinMapper.class);
        } else {
            System.err.println("Invalid program option.");
            System.exit(1);
        }
        createClusters(input, tmpDir + "/clusters");
        input = tmpDir + "/clusters";
        long counter = 1;
        int iteration = 0;

        while (counter > 0) {
            String output = parser.get("output") + iteration;
            counter = hashStrategy.hashStep(input, output);
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

}
