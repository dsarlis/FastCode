package mapreduce.strategy.smallstar;

import mapreduce.common.StarReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SmallStarReducer extends StarReducer {

    @Override
    protected void reduce(Iterable<Text> neighbors, Context context, String u, String m)
            throws IOException, InterruptedException {
        for (Text neighbor: neighbors) {
            context.write(neighbor, new Text(m));
        }
    }

}
