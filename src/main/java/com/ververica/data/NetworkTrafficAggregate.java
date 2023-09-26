package com.ververica.data;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class NetworkTrafficAggregate {
    private String ip;
    private Long windowCount;
    private Long windowStart;
}
