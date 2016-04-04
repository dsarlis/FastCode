package mapreduce.hashtomin;

import mapreduce.util.Constants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondStepMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable v, Text Cv, Context context) throws IOException, InterruptedException {
        String[] CvNodes  = Cv.toString().split(Constants.CLUSTER_SEPARATOR);
        int VminValue = Integer.MAX_VALUE;

        for (String u: CvNodes) {
            int node = Integer.parseInt(u);

            if (node < VminValue) {
                VminValue = node;
            }
        }
        String Vmin = String.format("%d", VminValue);

        for (String u: CvNodes) {
            //Emit (u, {Vmin})
            context.write(new Text(u), new Text(Vmin));
        }
        //Emit (Vmin, Cv)
        context.write(new Text(Vmin), Cv);
    }

}
