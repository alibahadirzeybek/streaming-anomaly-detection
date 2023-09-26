package com.ververica.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.ververica.data.NetworkTrafficAggregate;
import com.ververica.pipeline.NetworkTrafficAnomalyDetectionPipeline;

import java.io.IOException;

public class NetworkTrafficAnomalyDetectionProcessFunction
        extends KeyedProcessFunction<
        String,
        NetworkTrafficAggregate,
        NetworkTrafficAggregate> {
    private final Integer TRAINING_COUNT_LIMIT = 10;
    private final Float ALPHA = 0.85F;
    private final Float BETA = 1.0F;
    private final Float THRESHOLD_PERCENTAGE = 30.0F;

    private transient ValueState<Boolean> isAlertedState;
    private transient ValueState<Integer> trainingCountState;
    private transient ValueState<Double> movingAverageState;
    private transient ValueState<Double> movingStandardDeviationState;
    private transient ValueState<Double> s1State;
    private transient ValueState<Double> s2State;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> isAlertedDescriptor =
                new ValueStateDescriptor<>(
                        "is-alerted",
                        TypeInformation.of(new TypeHint<Boolean>() {}));
        ValueStateDescriptor<Integer> trainingCountDescriptor =
                new ValueStateDescriptor<>(
                        "training-count",
                        TypeInformation.of(new TypeHint<Integer>() {}));
        ValueStateDescriptor<Double> movingAverageDescriptor =
                new ValueStateDescriptor<>(
                        "moving-average",
                        TypeInformation.of(new TypeHint<Double>() {}));
        ValueStateDescriptor<Double> movingStandardDeviationDescriptor =
                new ValueStateDescriptor<>(
                        "moving-standard-deviation",
                        TypeInformation.of(new TypeHint<Double>() {}));
        ValueStateDescriptor<Double> s1Descriptor =
                new ValueStateDescriptor<>(
                        "s1",
                        TypeInformation.of(new TypeHint<Double>() {}));
        ValueStateDescriptor<Double> s2Descriptor =
                new ValueStateDescriptor<>(
                        "s2",
                        TypeInformation.of(new TypeHint<Double>() {}));
        isAlertedState = getRuntimeContext().getState(isAlertedDescriptor);
        trainingCountState = getRuntimeContext().getState(trainingCountDescriptor);
        movingAverageState = getRuntimeContext().getState(movingAverageDescriptor);
        movingStandardDeviationState = getRuntimeContext().getState(movingStandardDeviationDescriptor);
        s1State = getRuntimeContext().getState(s1Descriptor);
        s2State = getRuntimeContext().getState(s2Descriptor);
    }

    @Override
    public void processElement(
            NetworkTrafficAggregate networkTrafficAggregate,
            KeyedProcessFunction<
                    String,
                    NetworkTrafficAggregate,
                    NetworkTrafficAggregate>.Context context,
            Collector<NetworkTrafficAggregate> collector) throws IOException {
        detect(networkTrafficAggregate, collector);
        predict(networkTrafficAggregate, context);
    }

    private void detect(
            NetworkTrafficAggregate networkTrafficAggregate,
            Collector<NetworkTrafficAggregate> collector) throws IOException {
        final Boolean isAlerted = getAlerted();
        final Boolean isAnomaly = getAnomaly(networkTrafficAggregate.getWindowCount());
        if (!isAlerted && isAnomaly) {
            collectAlert(networkTrafficAggregate, collector);
            setAlerted(true);
        } else if(isAlerted && !isAnomaly) {
            setAlerted(false);
        }
    }

    private void predict(
            NetworkTrafficAggregate networkTrafficAggregate,
            KeyedProcessFunction<
                    String,
                    NetworkTrafficAggregate,
                    NetworkTrafficAggregate>.Context context) throws IOException {
        final Double currentValue = networkTrafficAggregate.getWindowCount().doubleValue();
        final Double alphaProbabilistic =
                getAlphaProbabilistic(
                        getTrainingCount(),
                        currentValue,
                        getMovingAverage(currentValue),
                        getMovingStandardDeviation());

        final Double s1 = getS1(currentValue);
        final Double s2 = getS2(Math.pow(currentValue, 2));

        final Double s1Next = (alphaProbabilistic * s1) + ((1 - alphaProbabilistic) * currentValue);
        final Double s2Next = (alphaProbabilistic * s2) + ((1 - alphaProbabilistic) * Math.pow(currentValue, 2));
        final Double movingAverageNext = s1Next;
        final Double movingStandardDeviationNext = Math.sqrt(s2Next - Math.pow(s1Next, 2));

        setS1(s1Next);
        setS2(s2Next);
        setMovingAverage(movingAverageNext);
        setMovingStandardDeviation(movingStandardDeviationNext);

        collectPrediction(
                movingAverageNext.longValue(),
                networkTrafficAggregate,
                context);
    }

    private void collectAlert(
            NetworkTrafficAggregate networkTrafficAggregate,
            Collector<NetworkTrafficAggregate> collector) {
        collector.collect(
                NetworkTrafficAggregate.builder()
                        .ip(networkTrafficAggregate.getIp())
                        .windowStart(networkTrafficAggregate.getWindowStart())
                        .windowCount(networkTrafficAggregate.getWindowCount())
                        .build());
    }

    private void collectPrediction(
            Long prediction,
            NetworkTrafficAggregate networkTrafficAggregate,
            KeyedProcessFunction<
                    String,
                    NetworkTrafficAggregate,
                    NetworkTrafficAggregate>.Context context) {
        context.output(
                NetworkTrafficAnomalyDetectionPipeline.networkTrafficAnomalyDetectionSideOutput,
                NetworkTrafficAggregate.builder()
                        .ip(networkTrafficAggregate.getIp())
                        .windowStart(networkTrafficAggregate.getWindowStart())
                        .windowCount(prediction)
                        .build());
    }

    private Double getAlphaProbabilistic(
            Integer trainingCount,
            Double currentValue,
            Double movingAverage,
            Double movingStandardDeviation) throws IOException {
        if (trainingCount < TRAINING_COUNT_LIMIT) {
            setTrainingCount(trainingCount + 1);
            return ((Integer)(1 - (1 / trainingCount))).doubleValue();
        } else {
            final Double zeta = (currentValue - movingAverage) / movingStandardDeviation;
            final Double probabilityDistribution = Math.exp(Math.pow(zeta, 2) / -2) / Math.sqrt(2 * Math.PI);
            return ALPHA * (1 - (BETA * probabilityDistribution));
        }
    }

    private Boolean getAnomaly(Long actualValue) throws IOException {
        final Integer trainingCount = getTrainingCount();
        if (trainingCount >= TRAINING_COUNT_LIMIT) {
            final Long expectedValue = getMovingAverage(0.0D).longValue();
            return ((Math.abs(actualValue - expectedValue) * 100.0F) / expectedValue) > THRESHOLD_PERCENTAGE;
        } else {
            return false;
        }
    }

    private Boolean getAlerted() throws IOException {
        final Boolean isAlerted = isAlertedState.value();
        if (isAlerted == null) {
            return false;
        } else {
            return isAlerted;
        }
    }

    private void setAlerted(Boolean isAlerted) throws IOException {
        isAlertedState.update(isAlerted);
    }

    private Double getMovingAverage(Double defaultValue) throws IOException {
        final Double movingAverage = movingAverageState.value();
        if (movingAverage ==  null) {
            return defaultValue;
        } else {
            return movingAverage;
        }
    }

    private void setMovingAverage(Double value) throws IOException {
        movingAverageState.update(value);
    }

    private Double getMovingStandardDeviation() throws IOException {
        final Double movingAverage = movingAverageState.value();
        if (movingAverage == null) {
            return 0.0;
        } else {
            return movingAverage;
        }
    }

    private void setMovingStandardDeviation(Double value) throws IOException {
        movingStandardDeviationState.update(value);
    }

    private Integer getTrainingCount() throws IOException {
        final Integer trainingCount = trainingCountState.value();
        if (trainingCount == null) {
            return 1;
        } else {
            return trainingCount;
        }
    }

    private void setTrainingCount(Integer value) throws IOException {
        trainingCountState.update(value);
    }

    private Double getS1(Double defaultValue) throws IOException {
        final Double s1 = s1State.value();
        if (s1 == null) {
            return defaultValue;
        } else {
            return s1;
        }
    }

    private void setS1(Double value) throws IOException {
        s1State.update(value);
    }

    private Double getS2(Double defaultValue) throws IOException {
        final Double s2 = s2State.value();
        if (s2 == null) {
            return defaultValue;
        } else {
            return s2;
        }
    }

    private void setS2(Double value) throws IOException {
        s2State.update(value);
    }
}
