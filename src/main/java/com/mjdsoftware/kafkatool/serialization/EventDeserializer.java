package com.mjdsoftware.kafkatool.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;

import java.util.Map;

@Slf4j
public class EventDeserializer<T> implements Deserializer<T> {

    @Getter(AccessLevel.PRIVATE)
    private final ObjectMapper objectMapper;

    @Getter(AccessLevel.PRIVATE) @Setter(AccessLevel.PRIVATE)
    private Class<T> tClass;

    /**
     * Answer my logger
     * @return Logger
     */
    private static Logger getLogger() {
        return log;
    }

    /**
     * Answer a default instance -- needed by kafka
     */
    public EventDeserializer() {

        super();
        objectMapper = new ObjectMapper();

    }

    /**
     * Configure this class.
     *
     * @param configs configs in key/value pairs
     * @param isKey   whether is for key or value
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

        if (isKey) {

            this.setTClass((Class<T>) String.class);
        }
        else {

            this.setTClass((Class<T>) EventObject.class);

        }
    }

    /**
     * Deserialize a record value from a byte array into a value or object.
     *
     * @param aTopic topic associated with the data
     * @param aData  serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    @Override
    public T deserialize(String aTopic,
                         byte[] aData) {

        T   tempResult = null;

        if (aData != null) {

            try {
                tempResult =
                        this.getObjectMapper().readValue(aData,
                                this.getTClass());
            }
            catch (Exception e) {

                getLogger().info("Deserialization error", e);
                throw new SerializationException(e);

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
