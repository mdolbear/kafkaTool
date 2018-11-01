package com.mjdsoftware.kafkatool.consumer;

import lombok.Value;
import reactor.core.Disposable;
import reactor.kafka.receiver.KafkaReceiver;

@Value
public class SubscriberRepositoryValue<K,V> {

    private Disposable disposable;
    private KafkaReceiver<K,V> receiver;


    /**
     * Cancel subscriber
     */
    public void cancelSubscriber() {

        this.getDisposable().dispose();

    }

}
