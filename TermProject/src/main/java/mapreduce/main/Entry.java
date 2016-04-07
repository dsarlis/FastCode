package mapreduce.main;


import mapreduce.strategy.HashGreaterToMinMapper;
import mapreduce.strategy.HashToAllMapper;
import mapreduce.strategy.HashToMinMapper;
import mapreduce.util.SimpleParser;

public class Entry {
    public static void main(String args[]) throws Exception  {
        SimpleParser parser = new SimpleParser(args);
        String program = parser.get("program");

        System.out.println("Running program " + program + "..");

        long start = System.currentTimeMillis();

        if (program.equals("hashToMin"))
            Driver.main(args, HashToMinMapper.class);
        else if (program.equals("hashToAll")) {
            Driver.main(args, HashToAllMapper.class);
        } else if (program.equals("hashGreaterToMin")) {
            Driver.main(args, HashGreaterToMinMapper.class);
        }
        else {
            System.out.println("Unknown program!");
            System.exit(1);
        }

        long end = System.currentTimeMillis();

        System.out.println(String.format("Runtime for program %s: %d minutes", program,
                (end - start)/1000/60));
    }
}

