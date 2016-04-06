package mapreduce.common;

import mapreduce.util.Constants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class MergeReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text node, Iterable<Text> Cvs, Context context)
            throws IOException, InterruptedException {
        Set<String> mergedCluster = new HashSet<String>();
        int maxSize = 0;
        //Merge all clusters into new one
        for (Text Cv: Cvs) {
            String[] CvElements =  Cv.toString().split(Constants.CLUSTER_SEPARATOR);

            if (CvElements.length > maxSize) {
                maxSize = CvElements.length;
            }
            Collections.addAll(mergedCluster, CvElements);
        }
        Iterator<String> iterator = mergedCluster.iterator();
        StringBuilder builder = new StringBuilder();

        if (iterator.hasNext()) {
            builder.append(iterator.next());

            while (iterator.hasNext()) {
                builder.append(Constants.CLUSTER_SEPARATOR).append(iterator.next());
            }
        }
        //Used to determinate stop condition
        //The new merged cluster has one or more new element(s)
        //http://codingwiththomas.blogspot.com/2011/04/controlling-hadoop-job-recursion.html
        if (maxSize < mergedCluster.size()) {
            context.getCounter(Constants.UpdateCounter.UPDATED).increment(1);
        }
        //Emit (v, merged Cvs)
        System.out.println("Node: " + node + " Cluster: " + builder.toString());
        context.write(node, new Text(builder.toString()));
    }

}
