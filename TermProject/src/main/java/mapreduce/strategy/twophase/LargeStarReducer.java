package mapreduce.strategy.twophase;

import mapreduce.common.StarReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class LargeStarReducer extends StarReducer {

    @Override
    protected boolean condition(String v, String u) {
        //Lv > Lu
        return v.compareTo(u) > 0;
    }

}
