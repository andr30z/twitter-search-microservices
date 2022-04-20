package com.microservices.demo.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.List;

@Data
@ConfigurationProperties(prefix = "twitter-to-kafka-service")
@Component
public class TwitterToKafkaServiceConfigData {
    private String welcomeMessage;
    private final List<String> twitterKeywords;
}
