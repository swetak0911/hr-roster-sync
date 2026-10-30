package com.moveinsync.roster.service;

import com.fasterxml.jackson.core.JsonProcessingException; import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.roster.domain.*; import com.moveinsync.roster.repository.*;
import org.springframework.stereotype.Service; import org.springframework.transaction.annotation.Transactional; import org.springframework.jdbc.core.JdbcTemplate; import org.springframework.jdbc.core.ConnectionCallback; import org.springframework.cache.annotation.CacheEvict;
import java.util.Set;

@Service public class EmployeeSyncService {
 private static final Set<String> VALID_OFFICES=Set.of("BLR","DEL","HYD","MUM","PUN","LKO","CHE");
 private final EmployeeRepository employees; private final OutboxRepository outbox; private final ObjectMapper mapper; private final JdbcTemplate jdbc; private final RosterMetrics metrics;
 public EmployeeSyncService(EmployeeRepository employees,OutboxRepository outbox,ObjectMapper mapper,JdbcTemplate jdbc,RosterMetrics metrics){this.employees=employees;this.outbox=outbox;this.mapper=mapper;this.jdbc=jdbc;this.metrics=metrics;}
 @Transactional @CacheEvict(cacheNames="employee-profile", key="#p0.tenantId() + ':' + #p0.employeeId()") public SyncResult apply(EmployeeEvent event) {
  if(event.type()!=EmployeeEvent.EventType.DEACTIVATE) {
   if(event.name()==null || event.name().isBlank() || event.email()==null || !event.email().matches("^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$") || event.officeCode()==null || event.officeCode().isBlank()) { metrics.rejected(); return new SyncResult(event.employeeId(),"REJECTED","INVALID_HR_FIELDS",-1); }
   if(event.phone()!=null && !event.phone().isBlank() && !event.phone().matches("^\\+?[1-9][0-9]{7,14}$")) { metrics.rejected(); return new SyncResult(event.employeeId(),"REJECTED","INVALID_PHONE",-1); }
   if(!VALID_OFFICES.contains(event.officeCode())) { metrics.rejected(); return new SyncResult(event.employeeId(),"REJECTED","UNKNOWN_OFFICE_CODE",-1); }
  }
  // Transaction-scoped advisory lock also serializes first-time creates (where no row exists to lock).
  jdbc.execute((ConnectionCallback<Void>) connection -> {
   try (var statement = connection.prepareStatement("select pg_advisory_xact_lock(hashtextextended(?, 0))")) {
    statement.setString(1, event.tenantId() + ":" + event.employeeId()); statement.execute();
   }
   return null;
  });
  Employee current=employees.findLocked(event.tenantId(),event.employeeId()).orElse(null);
  if(current!=null && event.version()<=current.getSourceVersion()) { metrics.ignored(); return new SyncResult(event.employeeId(),"IGNORED",event.version()==current.getSourceVersion()?"DUPLICATE_VERSION":"STALE_VERSION",current.getSourceVersion()); }
  boolean active=event.type()!=EmployeeEvent.EventType.DEACTIVATE;
  if(current==null){ if(!active) { metrics.ignored(); return new SyncResult(event.employeeId(),"IGNORED","DEACTIVATE_BEFORE_CREATE",-1); } current=new Employee(event.tenantId(),event.employeeId(),event.name(),event.email(),event.managerId(),event.officeCode(),event.phone(),event.version()); employees.save(current); }
  else if (!active) current.deactivate(event.version());
  else current.applyHr(event.name(),event.email(),event.managerId(),event.officeCode(),event.phone(),event.version(),true);
  // Transport-owned fields are deliberately never assigned by this HR path.
  employees.save(current);
  java.util.UUID eventId=java.util.UUID.randomUUID();
  String payload; try { payload=mapper.writeValueAsString(new ChangePayload(eventId.toString(),event.tenantId(),event.employeeId(),event.version(),event.type().name(),active)); } catch(JsonProcessingException ex){throw new IllegalStateException("Could not serialize change event",ex);}
  outbox.save(new OutboxEvent(eventId,event.tenantId(),event.employeeId(),active?"EMPLOYEE_CHANGED":"EMPLOYEE_DEACTIVATED",payload));
  metrics.applied(); return new SyncResult(event.employeeId(),"APPLIED","VERSION_ACCEPTED",event.version());
 }
 public record ChangePayload(String eventId,String tenantId,String employeeId,long version,String operation,boolean active){}
}
