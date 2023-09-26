package com.ververica.pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import com.ververica.data.NetworkTraffic;
import com.ververica.data.NetworkTrafficAggregate;
import com.ververica.function.NetworkTrafficAggregateProcessWindowFunction;
import com.ververica.function.NetworkTrafficAnomalyDetectionProcessFunction;

public class NetworkTrafficAnomalyDetectionPipeline {
    public static final OutputTag<NetworkTrafficAggregate> networkTrafficAnomalyDetectionSideOutput =
            new OutputTag<NetworkTrafficAggregate>("network-traffic-aggregate-prediction") {};
    public void run(
            StreamExecutionEnvironment streamExecutionEnvironment,
            SourceFunction<NetworkTraffic> networkTrafficSource,
            SinkFunction<NetworkTrafficAggregate> networkTrafficAggregateSink,
            SinkFunction<NetworkTrafficAggregate> networkTrafficAggregatePredictionSink,
            SinkFunction<NetworkTrafficAggregate> networkTrafficAnomalyDetectionSink) {

        final DataStream<NetworkTraffic> networkTraffic =
                streamExecutionEnvironment
                        .addSource(networkTrafficSource)
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<NetworkTraffic>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                (event, timestamp) ->
                                                        event.getTimestampInMilliseconds()));

        final DataStream<NetworkTrafficAggregate>  networkTrafficAggregate =
                networkTraffic
                        .keyBy(NetworkTraffic::getTargetIP)
                        .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                        .process(new NetworkTrafficAggregateProcessWindowFunction());

        final SingleOutputStreamOperator<NetworkTrafficAggregate> networkTrafficAnomalyDetection =
                networkTrafficAggregate
                        .keyBy(NetworkTrafficAggregate::getIp)
                        .process(new NetworkTrafficAnomalyDetectionProcessFunction());

        networkTrafficAnomalyDetection
                .getSideOutput(networkTrafficAnomalyDetectionSideOutput)
                .addSink(networkTrafficAggregatePredictionSink);

        networkTrafficAggregate.addSink(networkTrafficAggregateSink);

        networkTrafficAnomalyDetection.addSink(networkTrafficAnomalyDetectionSink);
    }
}
