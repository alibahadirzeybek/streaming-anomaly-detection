package com.ververica.data;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class NetworkTraffic {
    private String sourceIP;
    private String targetIP;
    private Long timestampInMilliseconds;
}
