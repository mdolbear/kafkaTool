package com.mjdsoftware.kafkatool.consumer;

import com.mjdsoftware.kafkatool.KafkaToolProperties;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class ReceiverOptionsBuilder<K,V> {

    @Getter(AccessLevel.PRIVATE)
    private final KafkaToolProperties properties;

    /**
     * Answer my logger
     * @return Logger
     */
    private static Logger getLogger() {
        return log;
    }

    /**
     * Answer a default instance
     * @param aProperties KafkaToolProperties
     */
    @Autowired
    public ReceiverOptionsBuilder(KafkaToolProperties aProperties) {

        super();
        this.properties = aProperties;
    }


    /**
     * Build my receiver options from the following
     * @param aBootstrapServers
     * @param aClientId
     * @param aGroupId
     * @param aDeserializerKeyClass
     * @param aDeserializerValueClass
     * @param aTopic
     * @return ReceiverOptions
     */
    public ReceiverOptions<K,V> build(String aBootstrapServers,
                                      String aClientId,
                                      String aGroupId,
                                      Class<?> aDeserializerKeyClass,
                                      Class<?> aDeserializerValueClass,
                                      String aTopic) {

        return this.createReceiverOptions(aBootstrapServers,
                                          aClientId,
                                          aGroupId,
                                          aDeserializerValueClass,
                                          aDeserializerKeyClass,
                                          aTopic);
    }


    /**
     * Create receiver options
     * @param aBootstrapServers
     * @param aClientId
     * @param aGroupId
     * @param aDeserializerKeyClass
     * @param aDeserializerValueClass
     * @param aTopic
     * @return ReceiverOptions
     */
    private ReceiverOptions<K,V> createReceiverOptions(String aBootstrapServers,
                                                       String aClientId,
                                                       String aGroupId,
                                                       Class<?> aDeserializerKeyClass,
                                                       Class<?> aDeserializerValueClass,
                                                       String aTopic) {

        ReceiverOptions<K,V>        tempOptions;
        Map<String, Object>         tempProperties;

        this.validateTopicSubscriberArguments(aBootstrapServers,
                aClientId,
                aGroupId,
                aDeserializerKeyClass,
                aDeserializerValueClass,
                aTopic);

        tempProperties =
                this.createConsumerProperties(aBootstrapServers,
                                              aClientId,
                                              aGroupId,
                                              aDeserializerKeyClass,
                                              aDeserializerValueClass);

        tempOptions =
                ReceiverOptions.create(tempProperties);
        tempOptions = tempOptions
                       .subscription(Collections.singleton(aTopic))
                       .commitInterval(Duration.ZERO)
                       .commitBatchSize(0)
                       .addAssignListener(partitions ->
                                                getLogger().debug("onPartitionsAssigned {}", partitions))
                       .addRevokeListener(partitions
                                            -> getLogger().debug("onPartitionsRevoked {}", partitions));

        return tempOptions;

    }

    /**
     * Validate topic subscriber arguments
     * @param aBootstrapServers
     * @param aClientId
     * @param aGroupId
     * @param aDeserializerKeyClass
     * @param aDeserializerValueClass
     * @param aTopic
     */
    private void validateTopicSubscriberArguments(String aBootstrapServers,
                                                  String aClientId,
                                                  String aGroupId, Class<?> aDeserializerKeyClass,
                                                  Class<?> aDeserializerValueClass,
                                                  String aTopic) {

        this.validateTopic(aTopic);
        this.validateAllConsumerProperties(aBootstrapServers,
                                           aClientId,
                                           aGroupId,
                                           aDeserializerKeyClass,
                                           aDeserializerValueClass);
    }

    /**
     * Create consumer properties
     * @param aBootstrapServers String
     * @param aClientId String
     * @param aGroupId String
     * @param aDeserializerKeyClass Class
     * @param aDeserializerValueClass Class
     * @return Map<String, Object>
     */
    private Map<String, Object> createConsumerProperties(String aBootstrapServers,
                                                         String aClientId,
                                                         String aGroupId,
                                                         Class<?> aDeserializerKeyClass,
                                                         Class<?> aDeserializerValueClass) {


        Map<String, Object> tempProperties = new HashMap<>();

        tempProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                           aBootstrapServers);
        tempProperties.put(ConsumerConfig.CLIENT_ID_CONFIG,
                           aClientId);
        tempProperties.put(ConsumerConfig.GROUP_ID_CONFIG,
                           aGroupId);
        tempProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                            aDeserializerKeyClass);
        tempProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                            aDeserializerValueClass);

        return tempProperties;
    }

    /**
     * Validate consumer properties
     * @param aBootstrapServers String
     * @param aClientId String
     * @param aGroupId String
     * @param aDeserializerKeyClass Class
     * @param aDeserializerValueClass Class
     */
    protected void validateAllConsumerProperties(String aBootstrapServers,
                                                 String aClientId,
                                                 String aGroupId,
                                                 Class<?> aDeserializerKeyClass,
                                                 Class<?> aDeserializerValueClass) {

        StringBuilder   tempBuilder = new StringBuilder();
        String          tempMsg;

        if (aBootstrapServers == null ||
                aBootstrapServers.isEmpty()) {

            tempBuilder.append("Bootstrap servers null or empty");
            tempBuilder.append(System.getProperty("line.separator"));
        }

        if (aClientId == null ||
                aClientId.isEmpty()) {

            tempBuilder.append("Client id null or empty");
            tempBuilder.append(System.getProperty("line.separator"));
        }

        if (aGroupId == null ||
                aGroupId.isEmpty()) {

            tempBuilder.append("Group id null or empty");
            tempBuilder.append(System.getProperty("line.separator"));
        }

        if (aDeserializerKeyClass == null) {

            tempBuilder.append("DeserializerKeyClass is null");
            tempBuilder.append(System.getProperty("line.separator"));
        }

        if (aDeserializerKeyClass == null) {

            tempBuilder.append("DeserializerValueClass is null");
            tempBuilder.append(System.getProperty("line.separator"));
        }

        tempMsg = tempBuilder.toString();
        if (!tempMsg.isEmpty()) {

            throw new IllegalArgumentException(tempMsg);
        }

    }


    /**
     * Validate topic
     * @param aTopic String
     */
    private void validateTopic(String aTopic) {

        if (aTopic == null ||
                aTopic.isEmpty()) {

            throw new IllegalArgumentException("Topic is null or empty");
        }

    }


}
