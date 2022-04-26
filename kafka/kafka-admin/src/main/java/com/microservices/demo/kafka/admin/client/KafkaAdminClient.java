package com.microservices.demo.kafka.admin.client;

import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.config.RetryConfigData;
import com.microservices.demo.kafka.admin.exception.KafkaClientException;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


@Component
public class KafkaAdminClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAdminClient.class);
    private final KafkaConfigData kafkaConfigData;

    private final AdminClient adminClient;

    private final WebClient webClient;
    private final RetryTemplate retryTemplate;
    private final RetryConfigData retryConfigData;

    public KafkaAdminClient(KafkaConfigData kafkaConfigData,
                            AdminClient adminClient,
                            WebClient webClient,
                            RetryTemplate retryTemplate,
                            RetryConfigData retryConfigData) {
        this.kafkaConfigData = kafkaConfigData;
        this.adminClient = adminClient;
        this.webClient = webClient;
        this.retryTemplate = retryTemplate;
        this.retryConfigData = retryConfigData;
    }

    public void createTopics() {
        CreateTopicsResult createTopicsResult;
        try {
            createTopicsResult = this.retryTemplate.execute(this::doCreateTopics);
        } catch (Throwable t) {
            throw new KafkaClientException("Reached max number of retry for " +
                    "creating kafka topic(s)!", t);
        }
        checkTopicsCreated();

    }

    private CreateTopicsResult doCreateTopics(RetryContext retryContext) {
        List<String> topicNames = kafkaConfigData.getTopicNamesToCreate();
        LOGGER.info("Creating {} topic(s), attempt {}.",
                topicNames.size(),
                retryContext.getRetryCount());

        List<NewTopic> kafkaTopics = topicNames.stream().map(topic ->
                new NewTopic(topic.trim(),
                        kafkaConfigData.getNumOfPartitions(),
                        kafkaConfigData.getReplicationFactor())).collect(Collectors.toList());
        return adminClient.createTopics(kafkaTopics);
    }

    private Collection<TopicListing> getTopics() {
        Collection<TopicListing> topics;
        try {

            topics = this.retryTemplate.execute(this::doGetTopics);
        } catch (Throwable t) {
            throw new KafkaClientException("Reached max number of retry for reading kafka topic(s)!", t);
        }
        return topics;
    }

    private Collection<TopicListing> doGetTopics(RetryContext retryContext) throws ExecutionException, InterruptedException {
        LOGGER.info("Reading kafka topic {}, attempt {}",
                kafkaConfigData.getTopicNamesToCreate()
                        .toArray(),
                retryContext.getRetryCount());
        Collection<TopicListing> topics = adminClient.listTopics().listings().get();
        if (topics != null) {
            topics.forEach(topic -> LOGGER.info("Topic with name {}.", topic.name()));
        }
        return topics;
    }

    public void checkTopicsCreated() {
        Collection<TopicListing> topics = getTopics();
        int retryCount = 1;
        Integer maxRetry = this.retryConfigData.getMaxAttempts();
        Integer multiplier = this.retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = this.retryConfigData.getSleepTimeMs();
        for (String topic : this.kafkaConfigData.getTopicNamesToCreate()) {
            while (!isTopicCreated(topics, topic)) {
                checkMaxRetry(retryCount++, maxRetry);
                sleep(sleepTimeMs);
                sleepTimeMs *= multiplier;
                topics = getTopics();
            }
        }
    }

    private void sleep(Long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException interruptedException) {
            throw new KafkaClientException("Error while sleeping for waiting new created topics!", interruptedException);
        }
    }

    private void checkMaxRetry(int retry, Integer maxRetry) {
        if (retry > maxRetry)
            throw new KafkaClientException("Reached max number of retry" +
                    " for reading kafka topic(s)!");

    }

    private boolean isTopicCreated(Collection<TopicListing> topics, String topicName) {
        return topics != null && topics.stream()
                .anyMatch(topic -> topic.name().equals(topicName));
    }

    private HttpStatus getSchemaRegistryStatus() {
        try {
            return webClient.method(HttpMethod.GET).uri(kafkaConfigData.getSchemaRegistryUrl()).
                    exchangeToMono(response -> response.statusCode().is2xxSuccessful() ?
                            Mono.just(response.statusCode())
                            :
                            Mono.just(HttpStatus.SERVICE_UNAVAILABLE))
                    .block();
        } catch (Exception e) {
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
    }

    public void checkSchemaRegistry() {
        int retryCount = 1;
        Integer maxRetry = this.retryConfigData.getMaxAttempts();
        Integer multiplier = this.retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = this.retryConfigData.getSleepTimeMs();
        while (getSchemaRegistryStatus().is2xxSuccessful()) {
            checkMaxRetry(retryCount++, maxRetry);
            sleep(sleepTimeMs);
            sleepTimeMs *= multiplier;
        }
    }
}
