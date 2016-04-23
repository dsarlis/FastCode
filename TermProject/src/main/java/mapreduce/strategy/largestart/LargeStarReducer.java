package mapreduce.strategy.largestart;

import mapreduce.common.StarReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class LargeStarReducer extends StarReducer {

    @Override
    protected void reduce(Iterable<Text> neighbors, Context context, String u, String m)
            throws IOException, InterruptedException {
        for (Text neighbor: neighbors) {
            String neighborStr = neighbor.toString();

            if (neighborStr.compareTo(u) > 0) {
                context.write(neighbor, new Text(m));
            }
        }
    }

}
