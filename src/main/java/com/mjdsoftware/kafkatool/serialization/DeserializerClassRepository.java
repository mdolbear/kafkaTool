package com.mjdsoftware.kafkatool.serialization;

import lombok.NonNull;

public interface DeserializerClassRepository {

    /**
     * Answer a deserializer class for aName
     * @param aName String
     * @return Class
     */
    Class<?> getClass(@NonNull String aName);
}
