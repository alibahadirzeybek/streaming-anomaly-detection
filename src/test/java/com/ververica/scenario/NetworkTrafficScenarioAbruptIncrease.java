package com.ververica.scenario;

import java.util.Arrays;
import java.util.List;

public class NetworkTrafficScenarioAbruptIncrease extends NetworkTrafficScenario {
    public NetworkTrafficScenarioAbruptIncrease(int startValue, int changeRate) {
        super(startValue, changeRate);
    }

    @Override
    public int getEpochCount() {
        return 3;
    }

    @Override
    public List<Integer> get() {
        int endValue = startValue * changeRate;
        int deltaValue = ((endValue - startValue) / 5) * 4;
        return Arrays.asList(startValue, startValue + deltaValue, endValue);
    }
}