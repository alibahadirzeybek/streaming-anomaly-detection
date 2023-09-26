package com.ververica.scenario;

import java.util.Arrays;
import java.util.List;

public class NetworkTrafficScenarioAbruptDecrease extends NetworkTrafficScenario {
    public NetworkTrafficScenarioAbruptDecrease(int startValue, int changeRate) {
        super(startValue, changeRate);
    }

    @Override
    public int getEpochCount() {
        return 3;
    }

    @Override
    public List<Integer> get() {
        int endValue = startValue / changeRate;
        int deltaValue = (startValue - endValue) / 5;
        return Arrays.asList(startValue, startValue - deltaValue, endValue);
    }
}