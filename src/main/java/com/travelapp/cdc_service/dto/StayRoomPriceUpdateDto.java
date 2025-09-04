package com.travelapp.cdc_service.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Set;

@Data
@AllArgsConstructor
public class StayRoomPriceUpdateDto {
    private Long stayId;
    private Set<RoomPriceDto> roomPriceDtoList;
}
