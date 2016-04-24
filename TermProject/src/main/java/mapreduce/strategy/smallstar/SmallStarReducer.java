package mapreduce.strategy.smallstar;

import mapreduce.common.StarReducer;

public class SmallStarReducer extends StarReducer {

    @Override
    protected boolean condition(String neighborStr, String u) {
        return true;
    }

}
