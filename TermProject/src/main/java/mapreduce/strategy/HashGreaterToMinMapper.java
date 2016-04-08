package mapreduce.strategy;

import mapreduce.util.Constants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HashGreaterToMinMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(Constants.SPACE_REGEX);
        String v = parts[0];
        String Cv = parts[1];
        String[] CvNodes  = Cv.split(Constants.CLUSTER_SEPARATOR);
        String Vmin = CvNodes[0];

        for (int i = 1; i < CvNodes.length; i++) {
            String node = CvNodes[i];

            if (node.compareTo(Vmin) < 0) {
                Vmin = node;
            }
        }
        StringBuilder CGTEV = new StringBuilder();
        for (String u: CvNodes) {
            //Emit (u, {Vmin})
            if (u.compareTo(v) >= 0) {
                CGTEV.append(u).append(Constants.CLUSTER_SEPARATOR);
                context.write(new Text(u), new Text(Vmin));
            }
        }
        //Emit (Vmin, C>=v)
        CGTEV.deleteCharAt(CGTEV.lastIndexOf(Constants.CLUSTER_SEPARATOR));
        context.write(new Text(Vmin), new Text(CGTEV.toString()));
    }

}
