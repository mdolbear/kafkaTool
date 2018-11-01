package com.mjdsoftware.kafkatool.consumer;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.function.Consumer;

@Slf4j
@Service
public class ConsumerServiceImpl<K,V> implements ConsumerService<K, V> {

    @Getter(AccessLevel.PRIVATE)
    private final ReceiverOptionsBuilder<K,V> builder;

    @Getter(AccessLevel.PRIVATE)
    private final SubscriberRepository<K,V> repository;


    /**
     * Answer my logger
     * @return Logger
     */
    private static Logger getLogger() {
        return log;
    }


    /**
     * Answer an instance of me for aReceiverOptions
     * @param aBuilder ReceiverOptionsBuilder
     */
    @Autowired
    public ConsumerServiceImpl(ReceiverOptionsBuilder<K,V> aBuilder,
                               SubscriberRepository<K,V> aRepository) {

        super();
        this.builder = aBuilder;
        this.repository = aRepository;

    }


    /**
     * Create topic Subscriber named aName
     * @param aName String
     * @param aBootstrapServers String
     * @param aClientId String
     * @param aGroupId String
     * @param aDeserializerKeyClass Class
     * @param aDeserializerValueClass Class
     * @param aTopic String
     */
    @Override
    public void createTopicSubscriber(@NonNull String aName,
                                      String aBootstrapServers,
                                      String aClientId,
                                      String aGroupId,
                                      Class<?> aDeserializerKeyClass,
                                      Class<?> aDeserializerValueClass,
                                      String aTopic,
                                      Consumer<ReceiverRecord<K, V>> aConsumer) {

        Flux<ReceiverRecord<K, V>>    tempFlux;
        Disposable                    tempDisposable;
        KafkaReceiver<K, V>           tempReceiver;

        this.getRepository().validateNumberOfSubscribersNotExceeded();

        tempReceiver =
                this.createReceiverFor(aBootstrapServers,
                                       aClientId,
                                       aGroupId,
                                       aDeserializerKeyClass,
                                       aDeserializerValueClass,
                                       aTopic);
        tempFlux = tempReceiver.receive();
        tempDisposable = tempFlux.subscribe(aConsumer);

        this.getRepository().addSubscriber(aName, tempDisposable, tempReceiver);

    }

    /**
     * Create a Kafka receiver for the following arguments
     * @param aBootstrapServers  String
     * @param aClientId String
     * @param aGroupId String
     * @param aDeserializerKeyClass Class
     * @param aDeserializerValueClass Class
     * @param aTopic String
     * @return KafkaReceiver
     */
    private KafkaReceiver<K, V> createReceiverFor(String aBootstrapServers,
                                                  String aClientId,
                                                  String aGroupId, Class<?> aDeserializerKeyClass,
                                                  Class<?> aDeserializerValueClass,
                                                  String aTopic) {
        KafkaReceiver<K, V> tempReceiver;
        ReceiverOptions<K, V> tempOptions;
        tempOptions = this.getBuilder().build(aBootstrapServers,
                                              aClientId,
                                              aGroupId,
                                              aDeserializerKeyClass,
                                              aDeserializerValueClass,
                                              aTopic);
        tempReceiver = KafkaReceiver.create(tempOptions);
        return tempReceiver;
    }


    /**
     * Terminate and remove topic subscriber for aName
     * @param aName String
     */
    public void terminateAndRemoveSubscriberFor(String aName) {

        this.getRepository().removeSubscriber(aName);

    }


}
