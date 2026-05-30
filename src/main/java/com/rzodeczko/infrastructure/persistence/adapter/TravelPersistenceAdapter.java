package com.rzodeczko.infrastructure.persistence.adapter;


import com.rzodeczko.application.port.out.TravelRepository;
import com.rzodeczko.domain.exception.OverbookingException;
import com.rzodeczko.domain.model.Booking;
import com.rzodeczko.domain.model.Hotel;
import com.rzodeczko.infrastructure.persistence.entity.DailyAvailabilityEntity;
import com.rzodeczko.infrastructure.persistence.mapper.TravelMapper;
import com.rzodeczko.infrastructure.persistence.repository.JpaBookingRepository;
import com.rzodeczko.infrastructure.persistence.repository.JpaDailyAvailabilityRepository;
import com.rzodeczko.infrastructure.persistence.repository.JpaHotelRepository;
import com.rzodeczko.infrastructure.persistence.repository.JpaOutboxRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;


@Component
@RequiredArgsConstructor
public class TravelPersistenceAdapter implements TravelRepository {

    private final JpaHotelRepository jpaHotelRepository;
    private final JpaBookingRepository jpaBookingRepository;
    private final JpaOutboxRepository jpaOutboxRepository;
    private final TravelMapper travelMapper;
    private final JpaDailyAvailabilityRepository jpaDailyAvailabilityRepository;

    @Override
    public Optional<Hotel> findHotel(Long id) {
        return jpaHotelRepository
                .findById(id)
                .map(travelMapper::toHotelDomain);
    }

    @Override
    public Booking save(Booking booking) {
        var entity = travelMapper.toBookingEntity(booking);
        var saved = jpaBookingRepository.save(entity);
        return travelMapper.toBookingDomain(saved);
    }

    @Override
    public void saveOutbox(Booking booking) {
        var outbox = travelMapper.toOutboxEntity(booking);
        jpaOutboxRepository.save(outbox);
    }


    @Override
    public void reserveAvailability(Long hotelId, int capacity, LocalDate start, LocalDate end) {

        Map<LocalDate, DailyAvailabilityEntity> existingSlots = jpaDailyAvailabilityRepository
                .findAndLockByHotelAndDateRange(hotelId, start, end)
                .stream()
                .collect(Collectors.toMap(DailyAvailabilityEntity::getDate, Function.identity()));


        List<DailyAvailabilityEntity> toSave = start
                .datesUntil(end.plusDays(1))
                .map(date -> reserveSlot(existingSlots, hotelId, date, capacity))
                .toList();

        jpaDailyAvailabilityRepository.saveAll(toSave);
    }

    private DailyAvailabilityEntity reserveSlot(
            Map<LocalDate, DailyAvailabilityEntity> existingSlots,
            Long hotelId,
            LocalDate date,
            int capacity
    ) {
        DailyAvailabilityEntity slot = existingSlots.computeIfAbsent(date, d ->
                DailyAvailabilityEntity
                        .builder()
                        .hotelId(hotelId)
                        .date(d)
                        .occupiedRooms(0)
                        .build()
        );

        if (slot.getOccupiedRooms() >= capacity) {
            throw new OverbookingException("Hotel %d overbooked on %s. Capacity: %d, occupied: %d".formatted(
                    hotelId, date, capacity, slot.getOccupiedRooms()
            ));
        }

        slot.setOccupiedRooms(slot.getOccupiedRooms() + 1);
        return slot;
    }

}
