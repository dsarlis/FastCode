package mapreduce.main;

import mapreduce.hashtomin.MergeReducer;
import mapreduce.job.OptimizedJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HashStrategy<T extends Mapper<?, ?, ?, ?>> {

    private Class<T> mapperClass;
    private String msg;

    public HashStrategy(Class<T> mapperClass) {
        this.mapperClass = mapperClass;
        this.msg = String.format("%s job", mapperClass.getSimpleName());
    }

    public long hashStep(String input, String output) throws InterruptedException, IOException, ClassNotFoundException {
        OptimizedJob job = new OptimizedJob(new Configuration(), input, output, msg);

        job.setClasses(mapperClass, MergeReducer.class, null);
        job.setMapOutputClasses(Text.class, Text.class);

        return job.run(true);
    }

}