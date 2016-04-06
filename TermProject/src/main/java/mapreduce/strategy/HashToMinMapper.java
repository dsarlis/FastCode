package mapreduce.strategy;

import mapreduce.util.Constants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HashToMinMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable v, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(Constants.SPACE_REGEX);
        String Cv = parts[1];
        String[] CvNodes  = Cv.split(Constants.CLUSTER_SEPARATOR);
        String Vmin = CvNodes[0];

        for (int i = 1; i < CvNodes.length; i++) {
            String node = CvNodes[i];

            if (node.compareTo(Vmin) < 0) {
                Vmin = node;
            }
        }
        for (String u: CvNodes) {
            //Emit (u, {Vmin})
            context.write(new Text(u), new Text(Vmin));
        }
        //Emit (Vmin, Cv)
        context.write(new Text(Vmin), new Text(Cv));
    }

}
