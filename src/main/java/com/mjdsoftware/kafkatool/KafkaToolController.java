package com.mjdsoftware.kafkatool;

import com.mjdsoftware.kafkatool.consumer.ConsumerFactory;
import com.mjdsoftware.kafkatool.consumer.ConsumerService;
import com.mjdsoftware.kafkatool.serialization.DeserializerClassRepository;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.function.Consumer;

@RestController
@RequestMapping("/kafkatool")
public class KafkaToolController {

    @Getter(AccessLevel.PRIVATE)
    private final DeserializerClassRepository deserializers;

    @Getter(AccessLevel.PRIVATE)
    private final ConsumerFactory consumerFactory;

    @Getter(AccessLevel.PRIVATE)
    private final ConsumerService<Object, Object> consumerService;

    /**
     * Answer an instance of me for the follwoing arguments
     * @param aDeserializers DeserializerClassRepository
     * @param aConsumerFactory ConsumerFactory
     * @param aConsumerService ConsumerService<Object, Object>
     */
    @Autowired
    public KafkaToolController(DeserializerClassRepository aDeserializers,
                               ConsumerFactory aConsumerFactory,
                               ConsumerService<Object, Object> aConsumerService) {

        super();
        this.deserializers = aDeserializers;
        this.consumerFactory = aConsumerFactory;
        this.consumerService = aConsumerService;

    }

    /**
     * Create a topic subscriber
     * @param aName String
     * @param aBootstrapServers String
     * @param aClientId String
     * @param aGroupId String
     * @param aDeserializerKeyClassShortName String
     * @param aDeserializerValueClassShortName String
     * @param aTopic String
     *
     * @return String
     */
    @PostMapping("/subscribe")
    public String createTopicSubscriber(@RequestParam("name") @NonNull String aName,
                                      @RequestParam("bootstrapServers") String aBootstrapServers,
                                      @RequestParam("clientId") String aClientId,
                                      @RequestParam("groupId") String aGroupId,
                                      @RequestParam("desKeyClassShortName") @NonNull String aDeserializerKeyClassShortName,
                                      @RequestParam("desValueClassShortName") @NonNull String aDeserializerValueClassShortName,
                                      @RequestParam("topic") String aTopic) {

        Class<?>                                 tempDeserializerKeyClass;
        Class<?>                                 tempDeserializerValueClass;
        Consumer<ReceiverRecord<Object, Object>> tempConsumer;

        tempDeserializerKeyClass = this.getDeserializerClass(aDeserializerKeyClassShortName);
        tempDeserializerValueClass = this.getDeserializerClass(aDeserializerValueClassShortName);
        tempConsumer = this.getConsumer("logger", //Placeholder, not used yet
                                         aClientId);

        this.getConsumerService().createTopicSubscriber(aName,
                                                aBootstrapServers,
                                                aClientId,
                                                aGroupId,
                                                tempDeserializerKeyClass,
                                                tempDeserializerValueClass,
                                                aTopic,
                                                tempConsumer);

        return aName;
    }

    /**
     * Terminate and remove topic subscriber for aName
     * @param aName String
     */
    @DeleteMapping("removeSubscriber")
    public void terminateAndRemoveSubscriber(@RequestParam("name") @NonNull String aName) {

        this.getConsumerService().terminateAndRemoveSubscriberFor(aName);
    }


    /**
     * Answer a consumer for a topic subscriber. There is currently only one type for this
     * @param aType String
     * @param aTopicConsumer String
     * @return Consumer
     */
    private Consumer<ReceiverRecord<Object, Object>> getConsumer(String aType,
                                                                 @NonNull  String aTopicConsumer) {

        return this.getConsumerFactory()
                        .getLoggingReceiverRecordConsumer(aTopicConsumer);

    }


    /**
     * Answer a deserializer class for aName or throw an exception
     * @param aName String
     * @return Class
     */
    private Class<?> getDeserializerClass(@NonNull String aName) {

        Class<?>    tempResult;

        tempResult = this.getDeserializers().getClass(aName);

        if (tempResult == null) {

            throw new IllegalArgumentException("No deserializer class found for " + aName);
        }

        return tempResult;
    }





}
