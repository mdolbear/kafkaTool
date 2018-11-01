package com.mjdsoftware.kafkatool.consumer;

import lombok.NonNull;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.function.Consumer;

public interface ConsumerService<K, V> {

    /**
     * Create a kafka subscriber for the following arguments. Save it in the repository
     * for aName
     * @param aName String
     * @param aBootstrapServers String
     * @param aClientId String
     * @param aGroupId String
     * @param aDeserializerKeyClass Class
     * @param aDeserializerValueClass Class
     * @param aTopic String
     * @param aConsumer Consumer
     */
    void createTopicSubscriber(@NonNull String aName,
                               String aBootstrapServers,
                               String aClientId,
                               String aGroupId,
                               Class<?> aDeserializerKeyClass,
                               Class<?> aDeserializerValueClass,
                               String aTopic,
                               Consumer<ReceiverRecord<K, V>> aConsumer);

    /**
     * Terminate and remove topic subscriber for aName
     * @param aName String
     */
    void terminateAndRemoveSubscriberFor(String aName);

}
