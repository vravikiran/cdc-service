package com.travelapp.cdc_service.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Data;

import java.util.Set;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class StayDetailDto {
    @JsonDeserialize(using = MongoIdDeserializer.class)
    @JsonProperty("_id")
    private Long id;
    private Set<RoomDetailDto> rooms;
    private boolean active = true;
    private boolean approved = true;
}
