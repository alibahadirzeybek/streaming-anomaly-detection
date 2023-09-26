package com.ververica.data;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class NetworkTrafficAnomalyDetectionTestReport {
    private List<NetworkTrafficAnomalyDetectionTestReportPoint> networkTrafficAggregate;
    private List<NetworkTrafficAnomalyDetectionTestReportPoint> networkTrafficAggregatePrediction;
    private List<NetworkTrafficAnomalyDetectionTestReportPoint> networkTrafficAnomalyDetection;
}
