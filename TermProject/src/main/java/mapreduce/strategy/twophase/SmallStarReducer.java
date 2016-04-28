package mapreduce.strategy.twophase;

import mapreduce.common.StarReducer;

import java.math.BigInteger;

public class SmallStarReducer extends StarReducer {

    @Override
    protected boolean condition(BigInteger neighborStr, BigInteger u) {
        // return all the neighbors
        return true;
    }

}
