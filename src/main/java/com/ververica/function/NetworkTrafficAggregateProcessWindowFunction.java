package com.ververica.function;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import com.ververica.data.NetworkTraffic;
import com.ververica.data.NetworkTrafficAggregate;

import java.util.stream.StreamSupport;

public class NetworkTrafficAggregateProcessWindowFunction
        extends ProcessWindowFunction<
        NetworkTraffic,
        NetworkTrafficAggregate,
        String,
        TimeWindow> {
    @Override
    public void process(
            String key,
            ProcessWindowFunction<
                    NetworkTraffic,
                    NetworkTrafficAggregate,
                    String,
                    TimeWindow>.Context context,
            Iterable<NetworkTraffic> iterable,
            Collector<NetworkTrafficAggregate> collector) {
        collector.collect(
                NetworkTrafficAggregate.builder()
                        .ip(key)
                        .windowCount(StreamSupport.stream(iterable.spliterator(), false).count())
                        .windowStart(context.window().getStart())
                        .build());
    }
}
