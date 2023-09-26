package com.ververica;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import com.ververica.data.NetworkTrafficAggregate;
import com.ververica.data.NetworkTrafficAnomalyDetectionTestReport;
import com.ververica.data.NetworkTrafficAnomalyDetectionTestReportPoint;
import com.ververica.function.NetworkTrafficAggregatePredictionSinkFunction;
import com.ververica.function.NetworkTrafficAggregateSinkFunction;
import com.ververica.function.NetworkTrafficAnomalyDetectionSinkFunction;
import com.ververica.function.NetworkTrafficSourceFunction;
import com.ververica.generator.NetworkTrafficGenerator;
import com.ververica.pipeline.NetworkTrafficAnomalyDetectionPipeline;
import com.ververica.scenario.NetworkTrafficScenario;
import com.ververica.scenario.NetworkTrafficScenarioAbruptDecrease;
import com.ververica.scenario.NetworkTrafficScenarioAbruptIncrease;
import com.ververica.scenario.NetworkTrafficScenarioNormal;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class NetworkTrafficAnomalyDetectionTest {
    @Test
    void test() throws Exception {
        runPipeline();
        generateReport();
    }

    private void runPipeline() throws Exception {
        final StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        new NetworkTrafficAnomalyDetectionPipeline().run(
                streamExecutionEnvironment,
                getSource(),
                new NetworkTrafficAggregateSinkFunction(),
                new NetworkTrafficAggregatePredictionSinkFunction(),
                new NetworkTrafficAnomalyDetectionSinkFunction());
        streamExecutionEnvironment.execute("Network Traffic Anomaly Detection Pipeline Test");
    }

    private NetworkTrafficSourceFunction getSource() {
        final List<NetworkTrafficScenario> networkTrafficScenarios = getNetworkTrafficScenarios();
        return NetworkTrafficSourceFunction.builder()
                .numberOfEpochs(getNumberOfEpochs(networkTrafficScenarios))
                .numberOfEventsForEpochs(getNumberOfEventsForEpochs(networkTrafficScenarios))
                .networkTrafficGenerator(new NetworkTrafficGenerator())
                .build();
    }

    private List<NetworkTrafficScenario> getNetworkTrafficScenarios() {
        final int averageNormal = 50;
        final int changeRate = 15;
        return Arrays.asList(
                // An underlying signal distribution
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                //  An abrupt transient change that is short-lived
                new NetworkTrafficScenarioAbruptIncrease(averageNormal, changeRate),
                new NetworkTrafficScenarioAbruptDecrease(averageNormal * changeRate, changeRate),
                // An underlying signal distribution
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                // An abrupt shift to the underlying signal distributional which is long-lived
                new NetworkTrafficScenarioAbruptIncrease(averageNormal, changeRate / 3),
                new NetworkTrafficScenarioNormal(averageNormal * changeRate / 3, changeRate / 3),
                new NetworkTrafficScenarioNormal(averageNormal * changeRate / 3, changeRate / 3),
                new NetworkTrafficScenarioNormal(averageNormal * changeRate / 3, changeRate / 3),
                new NetworkTrafficScenarioNormal(averageNormal * changeRate / 3, changeRate / 3),
                new NetworkTrafficScenarioNormal(averageNormal * changeRate / 3, changeRate / 3),
                new NetworkTrafficScenarioAbruptDecrease(averageNormal * changeRate / 3, changeRate / 3),
                // An underlying signal distribution
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate),
                new NetworkTrafficScenarioNormal(averageNormal, changeRate)
        );
    }

    private Integer getNumberOfEpochs(List<NetworkTrafficScenario> networkTrafficScenarios) {
        return networkTrafficScenarios.stream()
                .map(NetworkTrafficScenario::getEpochCount)
                .reduce(0, Integer::sum);
    }

    private List<Integer> getNumberOfEventsForEpochs(List<NetworkTrafficScenario> networkTrafficScenarios) {
        return networkTrafficScenarios.stream()
                .map(Supplier::get)
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    private void generateReport() throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.writeValue(
                new File(Paths.get("src/test/resources/report", "network-traffic-anomaly-detection.json").toUri()),
                NetworkTrafficAnomalyDetectionTestReport.builder()
                        .networkTrafficAggregate(generateReportPoints(NetworkTrafficAggregateSinkFunction.values))
                        .networkTrafficAggregatePrediction(generateReportPoints(NetworkTrafficAggregatePredictionSinkFunction.values))
                        .networkTrafficAnomalyDetection(generateReportPoints(NetworkTrafficAnomalyDetectionSinkFunction.values))
                        .build());
    }

    private List<NetworkTrafficAnomalyDetectionTestReportPoint> generateReportPoints(List<NetworkTrafficAggregate> networkTrafficAggregate) {
        return networkTrafficAggregate.stream()
                .map(element ->
                        NetworkTrafficAnomalyDetectionTestReportPoint.builder()
                                .xAxis(element.getWindowStart())
                                .yAxis(element.getWindowCount())
                                .build())
                .collect(Collectors.toList());
    }
}
