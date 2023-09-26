package com.ververica.function;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import com.ververica.data.NetworkTrafficAggregate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NetworkTrafficAggregateSinkFunction implements SinkFunction<NetworkTrafficAggregate> {
    public static final List<NetworkTrafficAggregate> values = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void invoke(NetworkTrafficAggregate networkTrafficAggregate, Context context) {
        values.add(networkTrafficAggregate);
    }
}
