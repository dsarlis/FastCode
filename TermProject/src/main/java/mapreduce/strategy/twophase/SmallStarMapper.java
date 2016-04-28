package mapreduce.strategy.twophase;

import mapreduce.util.Constants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.math.BigInteger;

public class SmallStarMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] edge = line.split(Constants.SPACE_REGEX);
        BigInteger u = new BigInteger(edge[0]);
        BigInteger v = new BigInteger(edge[1]);

        //if Lv <= Lu emit u,v
        // else emit v,u
        if (v.compareTo(u) <= 0) {
            context.write(new Text(u.toString()), new Text(v.toString()));
        } else {
            context.write(new Text(v.toString()), new Text(u.toString()));
        }
    }

}
