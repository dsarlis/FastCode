package mapreduce.strategy.twophase;

import mapreduce.util.Constants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TwoPhaseMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] edge = line.split(Constants.SPACE_REGEX);

        //Reverses edges
        context.write(new Text(edge[1]), new Text(edge[0]));
    }

}
