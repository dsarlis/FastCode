package mapreduce.strategy.smallstar;

import mapreduce.common.StarReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SmallStarReducer extends StarReducer {

    @Override
    protected boolean condition(String neighborStr, String u) {
        return true;
    }

}
