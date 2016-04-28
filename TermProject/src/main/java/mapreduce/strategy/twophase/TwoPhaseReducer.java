package mapreduce.strategy.twophase;

import mapreduce.util.Constants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class TwoPhaseReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text node, Iterable<Text> neighbors, Context context)
            throws IOException, InterruptedException {

        Set<String> nodes = new HashSet<>();
        nodes.add(node.toString());

        for (Text neighbor: neighbors) {
            nodes.add(neighbor.toString());
        }

        /* Merge together edges which belong to the same leader (thus to the same component) */
        StringBuilder sb = new StringBuilder();
        Iterator it = nodes.iterator();
        if (it.hasNext()) {
            sb.append(it.next());
            while (it.hasNext()) {
                sb.append(Constants.CLUSTER_SEPARATOR).append(it.next());
            }
        }

        context.write(node, new Text(sb.toString()));
    }
}

