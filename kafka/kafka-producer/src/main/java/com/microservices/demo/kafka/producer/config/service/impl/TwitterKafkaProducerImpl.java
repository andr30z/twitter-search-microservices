package com.microservices.demo.kafka.producer.config.service.impl;

import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import com.microservices.demo.kafka.producer.config.service.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.PreDestroy;

@Service
public class TwitterKafkaProducerImpl implements KafkaProducer<Long, TwitterAvroModel> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterKafkaProducerImpl.class);
    private KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;

    public TwitterKafkaProducerImpl(KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PreDestroy
    public void close() {
        if (kafkaTemplate != null) {
            LOGGER.info("Closing kafka producer");
            kafkaTemplate.destroy();
        }

    }

    @Override
    public void send(String topicName, Long key, TwitterAvroModel message) {
        LOGGER.info("Sending message='{}' to topic='{}'", message, topicName);
        ListenableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture =
                kafkaTemplate.send(topicName, key, message);

        addCallback(topicName, message, kafkaResultFuture);
    }

    private void addCallback(String topicName, TwitterAvroModel message, ListenableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture) {
        kafkaResultFuture.addCallback(new ListenableFutureCallback<SendResult<Long, TwitterAvroModel>>() {
            @Override
            public void onFailure(Throwable ex) {
                LOGGER.error("Error while sending message {} to topic {}",
                        message.toString(),
                        topicName,
                        ex
                );
            }

            @Override
            public void onSuccess(SendResult<Long, TwitterAvroModel> result) {
                RecordMetadata recordMetadata = result.getRecordMetadata();
                LOGGER.debug("Received new metadata. Topic: {}; Partition {}; " +
                                "Offset {}; Timestamp {}, at time {}.",
                        recordMetadata.topic(),
                        recordMetadata.partition(),
                        recordMetadata.offset(),
                        recordMetadata.timestamp(),
                        System.nanoTime()
                );
            }
        });
    }
}
