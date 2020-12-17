package com.clearpath.tweetpipe.kinesis.producer;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import twitter4j.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class TweetpipeProducerMain {

    public static void main(String[] args) {
        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(twitterListener());
        twitterStream.sample();
    }

    private static RawStreamListener twitterListener() {
        return new TweetStatusListener(createKinesisProducer());
    }

    private static KinesisProducer createKinesisProducer() {
        KinesisProducerConfiguration configuration = new KinesisProducerConfiguration()
                .setRequestTimeout(60000)
                .setRecordMaxBufferedTime(15000)
                .setRegion("eu-west-2");
        return new KinesisProducer(configuration);
    }

    private static class TweetStatusListener implements RawStreamListener {
        private static final Logger logger = LogManager.getLogger(TweetStatusListener.class);
        private final KinesisProducer kinesisProducer;

        public TweetStatusListener(KinesisProducer kinesisProducer) {
            this.kinesisProducer = kinesisProducer;
        }

        @Override
        public void onMessage(String rawMessage) {
            try {
                Status status = TwitterObjectFactory.createStatus(rawMessage);
                if (status.getUser() != null) {
                    logger.debug("Status text: {}", status.getText());
                    byte[] tweetBytes = rawMessage.getBytes(StandardCharsets.UTF_8);
                    String partitionKey = status.getLang();

                    ListenableFuture<UserRecordResult> f = kinesisProducer.addUserRecord(
                            "tweets-stream",
                            partitionKey,
                            ByteBuffer.wrap(tweetBytes)
                    );

                    Futures.addCallback(f, new FutureCallback<>() {
                        @Override
                        public void onSuccess(UserRecordResult userRecordResult) {
                            logger.debug("Record pushed to stream successfully");
                        }

                        @Override
                        public void onFailure(Throwable throwable) {
                            logger.error("Error: {}", throwable.getLocalizedMessage());
                        }
                    });
                }
            } catch (TwitterException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onException(Exception e) {
            logger.error("Error: {}", e);
        }
    }
}
