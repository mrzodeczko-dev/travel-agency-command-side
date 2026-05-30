package com.rzodeczko.infrastructure.persistence.repository;


import com.rzodeczko.infrastructure.persistence.entity.DailyAvailabilityEntity;
import com.rzodeczko.infrastructure.persistence.entity.DailyAvailabilityId;
import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;

import java.time.LocalDate;
import java.util.List;

public interface JpaDailyAvailabilityRepository extends JpaRepository<DailyAvailabilityEntity, DailyAvailabilityId> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints({@QueryHint(name = "jakarta.persistence.lock.timeout", value = "3000")})
    @Query("""
            select d from DailyAvailabilityEntity d
                where d.hotelId = :hotelId
                    and d.date >= :startDate
                        and d.date <= :endDate
            """)
    List<DailyAvailabilityEntity> findAndLockByHotelAndDateRange(Long hotelId, LocalDate startDate, LocalDate endDate);
}
