package com.mjdsoftware.kafkatool.consumer;

import reactor.core.Disposable;
import reactor.kafka.receiver.KafkaReceiver;

public interface SubscriberRepository<K, V> {

    /**
     * Add a subscriber to me
     * @param aName String
     * @param aDisposable Disposable
     * @param aReceiver KafkaReceiver
     */
    void addSubscriber(String aName,
                       Disposable aDisposable,
                       KafkaReceiver<K, V> aReceiver);

    /**
     * Ansser the subscriber for aName
     * @param aName String
     * @return SubscriberRepositoryValue
     */
    SubscriberRepositoryValue<K, V> getSubscriberFor(String aName);

    /**
     * Remove subscriber for aName
     * @param aName String
     */
    void removeSubscriber(String aName);

    /**
     * Validate number of subscribers not exceeded
     */
    void validateNumberOfSubscribersNotExceeded();

}
