package mapreduce.common;

import mapreduce.util.Constants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FirstStepReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context)
            throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();

        // Create a list of nodes in the Cluster
        builder.append(key.toString());
        for (Text v : value) {
            builder.append(Constants.CLUSTER_SEPARATOR).append(v.toString());
        }

        //Emit (node, {node} U node_neighbors)
        context.write(key, new Text(builder.toString()));
    }

}
