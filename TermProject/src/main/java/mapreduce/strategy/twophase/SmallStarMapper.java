package mapreduce.strategy.twophase;

import mapreduce.util.Constants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SmallStarMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] edge = line.split(Constants.SPACE_REGEX);
        String u = edge[0];
        String v = edge[1];

        if (u.compareTo(v) <= 0) {
            context.write(new Text(u), new Text(v));
        } else {
            context.write(new Text(v), new Text(u));
        }
    }

}
