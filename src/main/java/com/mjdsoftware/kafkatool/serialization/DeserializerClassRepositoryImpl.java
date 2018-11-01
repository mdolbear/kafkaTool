package com.mjdsoftware.kafkatool.serialization;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.Map;

@Repository
@Slf4j
public class DeserializerClassRepositoryImpl implements DeserializerClassRepository {

    @Getter(AccessLevel.PRIVATE)
    private final Map<String, Class<?>> classes;

    /**
     * Answer my logger
     * @return Logger
     */
    private static Logger getLogger() {
        return log;
    }

    /**
     * Answer a default instance with ny classes initialized
     */
    public DeserializerClassRepositoryImpl() {

        super();
        this.classes = new HashMap<String, Class<?>>();
        this.initialize();
    }

    /**
     * Initialize me
     */
    private void initialize() {

        this.getClasses().put(String.class.getSimpleName(),
                              StringDeserializer.class);

        this.getClasses().put(Integer.class.getSimpleName(),
                              IntegerDeserializer.class);
    }

    /**
     * Answer the class for aName
     * @param aName String
     * @return Class
     */
    @Override
    public Class<?> getClass(@NonNull String aName) {

        return this.getClasses().get(aName);
    }

}
