package mapreduce.strategy.twophase;

import mapreduce.common.StarReducer;

import java.math.BigInteger;

public class LargeStarReducer extends StarReducer {

    @Override
    protected boolean condition(BigInteger v, BigInteger u) {
        //Lv > Lu
        return v.compareTo(u) > 0;
    }

}
