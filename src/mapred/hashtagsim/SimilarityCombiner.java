package mapred.hashtagsim;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SimilarityCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> value,
                          Context context)
            throws IOException, InterruptedException {

        int similarity = 0;
        for (IntWritable v : value) {
            similarity += v.get();
        }

        context.write(key, new IntWritable(similarity));
    }

}
