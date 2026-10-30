package com.moveinsync.roster.repository;
import com.moveinsync.roster.domain.OutboxEvent; import org.springframework.data.jpa.repository.JpaRepository; import java.util.*;
public interface OutboxRepository extends JpaRepository<OutboxEvent,UUID> { List<OutboxEvent> findTop100ByPublishedAtIsNullOrderByCreatedAtAsc(); long countByPublishedAtIsNull(); java.util.Optional<OutboxEvent> findFirstByPublishedAtIsNullOrderByCreatedAtAsc(); }
