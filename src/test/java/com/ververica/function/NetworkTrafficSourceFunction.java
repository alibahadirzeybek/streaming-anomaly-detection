package com.ververica.function;

import lombok.Builder;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.ververica.data.NetworkTraffic;

import java.util.List;
import java.util.function.BiFunction;

@Builder
public class NetworkTrafficSourceFunction implements SourceFunction<NetworkTraffic> {
    private Integer numberOfEpochs;
    private List<Integer> numberOfEventsForEpochs;
    private BiFunction<Integer, Integer, List<NetworkTraffic>> networkTrafficGenerator;

    @Override
    public void run(SourceContext<NetworkTraffic> sourceContext) {
        Integer numberOfEpochsOriginal = numberOfEpochs;
        while (numberOfEpochs >= 0) {
            int epochIndex = numberOfEpochsOriginal - numberOfEpochs;
            collect(sourceContext, epochIndex, numberOfEventsForEpochs.get(epochIndex));
            setNumberOfEpochs();
        }
    }

    @Override
    public void cancel() {
        numberOfEpochs = -1;
    }

    private void collect(SourceContext<NetworkTraffic> sourceContext,
                         Integer epochIndex,
                         Integer numberOfEventsForEpoch) {
        networkTrafficGenerator
                .apply(epochIndex, numberOfEventsForEpoch)
                .forEach(sourceContext::collect);
    }

    private void setNumberOfEpochs() {
        if (numberOfEpochs == 1) {
            numberOfEpochs = -1;
        } else if (numberOfEpochs > 1) {
            numberOfEpochs -= 1;
        }
    }
}
