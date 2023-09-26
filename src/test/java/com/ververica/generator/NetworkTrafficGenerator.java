package com.ververica.generator;

import com.ververica.data.NetworkTraffic;

import java.io.Serializable;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class NetworkTrafficGenerator implements BiFunction<Integer, Integer, List<NetworkTraffic>>, Serializable {
    @Override
    public List<NetworkTraffic> apply(Integer epochIndex, Integer numberOfEvents) {
        long timestampInMillisecondsDelta = 1000L / (numberOfEvents - 1);
        return IntStream.range(0, numberOfEvents)
                .mapToObj(eventIndex ->
                        generate(epochIndex,
                                eventIndex,
                                timestampInMillisecondsDelta))
                .collect(Collectors.toList());
    }

    private NetworkTraffic generate(Integer epochIndex,
                                    Integer eventIndex,
                                    Long timestampInMillisecondsDelta) {
        return NetworkTraffic.builder()
                .sourceIP("SOURCE_IP")
                .targetIP("TARGET_IP")
                .timestampInMilliseconds(
                        System.currentTimeMillis()
                                + (epochIndex * 1000L)
                                + (eventIndex * timestampInMillisecondsDelta))
                .build();
    }
}
