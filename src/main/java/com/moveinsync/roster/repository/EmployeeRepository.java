package com.moveinsync.roster.repository;
import com.moveinsync.roster.domain.Employee; import org.springframework.data.jpa.repository.*; import org.springframework.data.repository.query.Param; import jakarta.persistence.LockModeType; import jakarta.persistence.QueryHint; import java.util.*; import java.util.stream.Stream;
public interface EmployeeRepository extends JpaRepository<Employee,Long> {
 Optional<Employee> findByTenantIdAndEmployeeId(String tenantId,String employeeId);
 @Lock(LockModeType.PESSIMISTIC_WRITE) @Query("select e from Employee e where e.tenantId=:tenant and e.employeeId=:employee") Optional<Employee> findLocked(@Param("tenant") String tenant,@Param("employee") String employee);
 long countByTenantIdAndActiveTrue(String tenantId);
 @Query("select e.employeeId from Employee e where e.tenantId=:tenant and e.active=true") @QueryHints(@QueryHint(name="org.hibernate.fetchSize", value="1000")) Stream<String> streamActiveIds(@Param("tenant") String tenant);
}
