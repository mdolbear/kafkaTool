package com.mjdsoftware.kafkatool.serialization;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor(force = true)
@AllArgsConstructor
public class EventObject {

    private String description;
    private String data;

}
