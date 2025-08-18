package com.travelapp.cdc_service.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class RoomDetailDto {
    private int roomId;
    private int numberOfRooms;
}
