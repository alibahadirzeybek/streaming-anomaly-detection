package com.ververica.scenario;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class NetworkTrafficScenarioNormal extends NetworkTrafficScenario {
    public NetworkTrafficScenarioNormal(int startValue, int changeRate) {
        super(startValue, changeRate);
    }

    @Override
    public int getEpochCount() {
        return 10;
    }

    @Override
    public List<Integer> get() {
        int fluctuation = (startValue * changeRate) / 100;
        Random randomGenerator = new Random();
        return IntStream.range(0, getEpochCount())
                .mapToObj(index -> startValue + randomGenerator.nextInt(2 * fluctuation + 1) - fluctuation)
                .collect(Collectors.toList());
    }
}