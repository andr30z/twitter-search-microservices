package com.microservices.demo.twitter.to.kafka.service.init;

import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.kafka.admin.client.KafkaAdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class KafkaStreamInitializerImpl implements StreamInitializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamInitializerImpl.class);

    private final KafkaConfigData kafkaConfigData;

    private final KafkaAdminClient kafkaAdminClient;

    public KafkaStreamInitializerImpl(KafkaConfigData kafkaConfigData,
                                      KafkaAdminClient kafkaAdminClient) {
        this.kafkaConfigData = kafkaConfigData;
        this.kafkaAdminClient = kafkaAdminClient;
    }


    @Override
    public void init() {
        this.kafkaAdminClient.createTopics();
        this.kafkaAdminClient.checkSchemaRegistry();
        LOGGER.info("Topic with name {} is ready for operations!",
                this.kafkaConfigData.getTopicNamesToCreate().toArray());
    }
}
