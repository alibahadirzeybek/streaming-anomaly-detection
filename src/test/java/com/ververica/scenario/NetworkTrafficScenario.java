package com.ververica.scenario;

import java.util.List;
import java.util.function.Supplier;

public abstract class NetworkTrafficScenario implements Supplier<List<Integer>> {
    protected final int startValue;
    protected final int changeRate;
    protected NetworkTrafficScenario(int startValue, int changeRate) {
        this.startValue = startValue;
        this.changeRate = changeRate;
    }
    public abstract int getEpochCount();
}
