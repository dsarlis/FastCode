package mapreduce.main;

import mapreduce.job.OptimizedJob;
import mapreduce.strategy.largestar.LargeStarMapper;
import mapreduce.strategy.largestar.LargeStarReducer;
import mapreduce.strategy.smallstar.SmallStarMapper;
import mapreduce.strategy.smallstar.SmallStarReducer;
import mapreduce.util.ChecksumChecker;
import mapreduce.util.SimpleParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class TwoPhaseDriver {

    public static void main(String[] args) throws Exception {
        SimpleParser parser = new SimpleParser(args);
        String input = parser.get("input");
        String output = parser.get("output");

        ChecksumChecker checker = new ChecksumChecker();

        int largeIteration = -1;
        int smallIteration = -1;
        String currentOutput;
        String[] parts;
        do {
            do {
                largeIteration++;
                currentOutput = output + "-largeStar-" + largeIteration;
                starJob(input, currentOutput, "Large Star Job", LargeStarMapper.class, LargeStarReducer.class);
                input = currentOutput;
                parts = currentOutput.split("/");
            } while (checker.checkSumsChanged(parts[parts.length-1]));
            smallIteration++;
            currentOutput = output + "-smallStar-" + smallIteration;
            starJob(input, currentOutput, "Small Star Job", SmallStarMapper.class,
                    SmallStarReducer.class);
            input = currentOutput;
            parts = currentOutput.split("/");
        } while (checker.checkSumsChanged(parts[parts.length-1]));
    }

    private static void starJob(String input, String output, String jobName, Class mapper, Class reducer) throws Exception {
        OptimizedJob job = new OptimizedJob(new Configuration(), input, output, jobName);
        job.setClasses(mapper, reducer, null);
        job.setMapOutputClasses(Text.class, Text.class);
        job.run(false);
    }
}
