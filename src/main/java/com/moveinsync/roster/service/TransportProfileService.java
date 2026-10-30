package com.moveinsync.roster.service;
import com.moveinsync.roster.repository.EmployeeRepository; import org.springframework.stereotype.Service; import org.springframework.cache.annotation.CacheEvict; import org.springframework.transaction.annotation.Transactional;
@Service public class TransportProfileService { private final EmployeeRepository repo; public TransportProfileService(EmployeeRepository repo){this.repo=repo;}
 @Transactional @CacheEvict(cacheNames="employee-profile", key="#p0 + ':' + #p1") public void update(String tenant,String employee,String pickup,boolean assistance){var e=repo.findLocked(tenant,employee).orElseThrow(()->new IllegalArgumentException("EMPLOYEE_NOT_FOUND"));e.updateTransport(pickup,assistance);}
}
