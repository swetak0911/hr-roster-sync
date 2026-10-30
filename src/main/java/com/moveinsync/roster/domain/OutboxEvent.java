package com.moveinsync.roster.domain;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity @Table(name="outbox_events", indexes=@Index(name="idx_outbox_pending",columnList="published_at,created_at"))
public class OutboxEvent {
 @Id private UUID id; @Column(name="tenant_id",nullable=false) private String tenantId; @Column(name="employee_id",nullable=false) private String employeeId;
 @Column(name="event_type",nullable=false) private String eventType; @Column(name="payload",nullable=false,columnDefinition="text") private String payload;
 @Column(name="created_at",nullable=false) private Instant createdAt; @Column(name="published_at") private Instant publishedAt; @Column(nullable=false) private int attempts=0;
 protected OutboxEvent(){} public OutboxEvent(UUID id,String tenant,String employee,String type,String payload){this.id=id;this.tenantId=tenant;this.employeeId=employee;this.eventType=type;this.payload=payload;this.createdAt=Instant.now();}
 public UUID getId(){return id;} public String getTenantId(){return tenantId;} public String getEmployeeId(){return employeeId;} public String getEventType(){return eventType;} public String getPayload(){return payload;} public Instant getCreatedAt(){return createdAt;} public Instant getPublishedAt(){return publishedAt;} public int getAttempts(){return attempts;} public void markPublished(){publishedAt=Instant.now();} public void incrementAttempts(){attempts++;}
}
