package com.travelapp.cdc_service.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RoomPriceDto {
    private int roomId;
    private double price;
}
