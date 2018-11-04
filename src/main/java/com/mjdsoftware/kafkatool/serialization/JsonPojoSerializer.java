package com.mjdsoftware.kafkatool.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;

import java.util.Map;

@Slf4j
public class JsonPojoSerializer<T>  implements Serializer<T> {

    @Getter(AccessLevel.PRIVATE)
    private final ObjectMapper objectMapper;

    /**
     * Answer my logger
     * @return Logger
     */
    private static Logger getLogger() {
        return log;
    }

    /**
     * Default constructor needed by Kafka
     */
    public JsonPojoSerializer() {

        super();
        objectMapper = new ObjectMapper();

    }

    /**
     * Configure does nothing
     * @param props Map
     * @param isKey boolean
     */
    @Override
    public void configure(Map<String, ?> props,
                          boolean isKey) {

        //Do nothing
    }

    /**
     * Serialize aData for
     * @param aTopic String
     * @param aData T
     * @return byte[]
     */
    @Override
    public byte[] serialize(String aTopic,
                            T aData) {

        byte[]  tempResult = null;

       if (aData != null) {

           try {
               tempResult = this.getObjectMapper().writeValueAsBytes(aData);
           }
           catch (Exception e) {

               getLogger().info("Error serializing JSON message", e);
               throw new SerializationException("Error serializing JSON message", e);
           }

       }

       return tempResult;

    }

    /**
     * Close does nothing
     */
    @Override
    public void close() {

        //Do nothing
    }


}
