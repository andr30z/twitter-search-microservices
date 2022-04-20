package com.microservices.demo.twitter.to.kafka.service.runner;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import javax.annotation.PreDestroy;
import java.util.Arrays;

@Component
public class TwitterStreamRunnerImpl implements TwitterStreamRunner {
    private final Logger LOG = LoggerFactory.getLogger(TwitterStreamRunnerImpl.class);
    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;
    private TwitterStream twitterStream;

    public TwitterStreamRunnerImpl(
            TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData,
            TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }

    @Override
    public void start() throws TwitterException {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("UaPxDWrNXQJRtxDBGjKrqmIcd")
                .setOAuthConsumerSecret("wsB8t9m4MT7VXs2UK4YaIV8tA02acDwHAfREyZuFdQo25UYle6")
                .setOAuthAccessToken("1181602748980236289")
                .setOAuthAccessTokenSecret("Y9BCvb1AhiPI8QdDKUyglXJD6tQ2eHgkehf1PixdGz0LR");
        var twitterStreamFactory = new TwitterStreamFactory(cb.build());
        twitterStream = twitterStreamFactory.getInstance();
        twitterStream.addListener(twitterKafkaStatusListener);
        addFilter();
    }

    @PreDestroy
    public void shutdown() {
        if (twitterStream == null) return;
        LOG.info("Closing twitter stream!");
        twitterStream.shutdown();
    }

    private void addFilter() {
        String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
        LOG.info(
                "Started filtering twitter stream for keywords {}",
                Arrays.toString(keywords));
    }

}
