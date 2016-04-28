package mapreduce.main;

import mapreduce.job.OptimizedJob;
import mapreduce.strategy.twophase.*;
import mapreduce.util.ChecksumChecker;
import mapreduce.util.SimpleParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class TwoPhaseDriver {

    public static void main(String[] args) throws Exception {
        SimpleParser parser = new SimpleParser(args);
        String input = parser.get("input");
        String output = parser.get("output");

        /* Class used to check if the output files have
         * changed from one iteration to the next one
         */
        ChecksumChecker checker = new ChecksumChecker();

        int largeIteration = -1;
        int smallIteration = -1;
        String currentOutput;
        String[] parts;
        /* Repeat large star and small star operations until convergence
         * For more information, check this: http://dl.acm.org/citation.cfm?id=2670997
         */
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

        /* Final job to merge connected components together */
        starJob(input, output, "Merge job", TwoPhaseMapper.class, TwoPhaseReducer.class);
    }

    /**
     * Generic star-job, either a large star or small star
     * @param input
     * @param output
     * @param jobName
     * @param mapper
     * @param reducer
     * @throws Exception
     */
    private static void starJob(String input, String output, String jobName, Class mapper, Class reducer) throws Exception {
        OptimizedJob job = new OptimizedJob(new Configuration(), input, output, jobName);
        job.setClasses(mapper, reducer, null);
        job.setMapOutputClasses(Text.class, Text.class);
        job.run(false);
    }
}
