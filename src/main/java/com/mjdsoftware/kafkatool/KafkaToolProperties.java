package com.mjdsoftware.kafkatool;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "app")
public class KafkaToolProperties {

    private String applicationName;
    private String applicationDescription;
    private Integer maxConsumers;

}
