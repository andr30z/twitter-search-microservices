package com.microservices.demo.twitter.to.kafka.service;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.runner.TwitterStreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.util.Arrays;

@SpringBootApplication
@ComponentScan(basePackages = "com.microservices.demo")
public class TwitterToKafkaServiceApplication implements CommandLineRunner {
    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    private final TwitterStreamRunner twitterStreamRunner;
    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);

    public TwitterToKafkaServiceApplication(
            TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData,
            TwitterStreamRunner twitterStreamRunner
    ) {
        this.twitterStreamRunner = twitterStreamRunner;
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
    }

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        LOG.info("App starts");
//        LOG.info(twitterStreamRunner.toString()+"TWITTER");
        LOG.info(twitterToKafkaServiceConfigData.toString());
        LOG.info(Arrays.toString(
                twitterToKafkaServiceConfigData
                        .getTwitterKeywords()
                        .toArray(new String[0])));
        LOG.info(twitterToKafkaServiceConfigData.getWelcomeMessage());
        twitterStreamRunner.start();
    }
}
