package com.mjdsoftware.kafkatool.consumer;


import com.mjdsoftware.kafkatool.KafkaToolProperties;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import reactor.core.Disposable;
import reactor.kafka.receiver.KafkaReceiver;

import java.util.HashMap;
import java.util.Map;

@Repository
@Slf4j
public class SubscriberRepositoryImpl<K,V> implements SubscriberRepository<K, V> {

    @Getter(AccessLevel.PRIVATE)
    private final KafkaToolProperties properties;

    @Getter(AccessLevel.PRIVATE)
    private final Map<String, SubscriberRepositoryValue<K,V>> subscribers;

    private final Object accessLock = new Object();

    /**
     * Answer my logger
     * @return Logger
     */
    private static Logger getLogger() {
        return log;
    }

    /**
     * Answer an instance of me for aProperties
     * @param aProperties KafkaTooProperties
     */
    @Autowired
    public SubscriberRepositoryImpl(KafkaToolProperties aProperties) {

        super();
        this.properties = aProperties;
        this.subscribers = new HashMap<String, SubscriberRepositoryValue<K,V>>();

    }

    /**
     * Add subscriber
     * @param aName String
     * @param aDisposable Disposable
     * @param aReceiver KafkaReceiver
     */
    @Override
    @Synchronized("accessLock")
    public void addSubscriber(String aName,
                              Disposable aDisposable,
                              KafkaReceiver<K, V> aReceiver) {

        SubscriberRepositoryValue<K,V> tempValue;

        this.checkForSubscriberAndCancelIfExists(aName);
        tempValue = new SubscriberRepositoryValue<>(aDisposable, aReceiver);
        this.getSubscribers().put(aName, tempValue);

    }

    /**
     * Answer a subscriber value for aName and cancel it if it exists
     * @param aName  String
     * @return SubscriberRepsositoryValue
     */
    private SubscriberRepositoryValue<K, V> checkForSubscriberAndCancelIfExists(String aName) {

        SubscriberRepositoryValue<K, V> tempValue;

        tempValue = this.getSubscriberFor(aName);
        if (tempValue != null) {

            tempValue.cancelSubscriber();
        }

        return tempValue;

    }

    /**
     * Answer my subscriber for aName
     * @param aName String
     * @return SubscriberRepositoryValue
     *
     */
    @Override
    @Synchronized("accessLock")
    public SubscriberRepositoryValue<K,V> getSubscriberFor(String aName) {

        return this.getSubscribers().get(aName);
    }

    /**
     * Remove subscriber named aName
     * @param aName String
     */
    @Override
    @Synchronized("accessLock")
    public void removeSubscriber(String aName) {

        SubscriberRepositoryValue<K, V> tempValue;

        tempValue = this.checkForSubscriberAndCancelIfExists(aName);
        if (tempValue != null) {

            this.getSubscribers().remove(aName);
        }

    }

    /**
     * Answer the max number of subscribers
     * @return Integer
     */
    private Integer getMaxNumberOfSubscribers() {

        return this.getProperties().getMaxConsumers();
    }

    /**
     * Answer the current number of subscribers
     * @return Integer
     */
    private Integer getCurrentNumberOfSubscribers() {

        return new Integer(this.getSubscribers().size());
    }

    /**
     * Validate number of subscribers not exceeded
     */
    public void validateNumberOfSubscribersNotExceeded() {

        int tempNewNumber;

        tempNewNumber = this.getCurrentNumberOfSubscribers() + 1;
        if (tempNewNumber > this.getMaxNumberOfSubscribers().intValue()) {

            throw new IllegalArgumentException("Cannot add another subscriber. Current max: "
                    + this.getMaxNumberOfSubscribers());
        }

    }


}
