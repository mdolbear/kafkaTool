package com.mjdsoftware.kafkatool.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.function.Consumer;

@Component
public class ConsumerFactory {

    //Constants
    private static final String TOPIC_CONSUMER_MDC_KEY = "topicConsumer";

    /**
     * Answer a default instance
     */
    public ConsumerFactory() {

        super();
    }

    /**
     * Answer a logging consumer
     */
    public <K, V> Consumer<ReceiverRecord<K, V>>
                getLoggingReceiverRecordConsumer(String aTopicConsumer) {

        return r -> {

            Logger tempLogger =
                    LoggerFactory.getLogger(ConsumerFactory.class);

            MDC.put(TOPIC_CONSUMER_MDC_KEY, aTopicConsumer);
            tempLogger.info("Received message: %s\n", r.toString());

            r.receiverOffset().acknowledge();

        };

    }

}
