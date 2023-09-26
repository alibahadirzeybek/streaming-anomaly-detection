package com.ververica.data;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class NetworkTrafficAnomalyDetectionTestReportPoint {
    private Long xAxis;
    private Long yAxis;
}
