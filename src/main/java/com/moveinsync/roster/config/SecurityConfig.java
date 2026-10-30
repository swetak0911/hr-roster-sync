package com.moveinsync.roster.config;

import com.moveinsync.roster.security.TenantPrincipal;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;

import java.util.Map;

@Configuration
@EnableMethodSecurity
public class SecurityConfig {

    @Bean
    SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        return http.csrf(c -> c.disable())
            .authorizeHttpRequests(a -> a.requestMatchers("/actuator/health", "/actuator/info").permitAll().anyRequest().authenticated())
            .httpBasic(Customizer.withDefaults()).build();
    }

    @Bean
    UserDetailsService users(PasswordEncoder encoder,
            @Value("${OPERATOR_PASSWORD:change-me}") String operatorPassword,
            @Value("${TRANSPORT_ADMIN_PASSWORD:change-me}") String transportPassword,
            @Value("${ADMIN_PASSWORD:change-me}") String adminPassword,
            @Value("${GLOBEX_OPERATOR_PASSWORD:globex-change-me}") String globexPassword,
            @Value("${EMPLOYEE_PASSWORD:employee-change-me}") String employeePassword) {
        Map<String, TenantPrincipal> map = Map.of(
            "operator", user("operator", operatorPassword, "acme", "OPERATOR", null, encoder),
            "transport-admin", user("transport-admin", transportPassword, "acme", "TRANSPORT_ADMIN", null, encoder),
            "admin", user("admin", adminPassword, "acme", "ADMIN", null, encoder),
            "globex-operator", user("globex-operator", globexPassword, "globex", "OPERATOR", null, encoder),
            "employee-e1001", user("employee-e1001", employeePassword, "acme", "EMPLOYEE", "E-1001", encoder));

        return username -> {
            TenantPrincipal p = map.get(username.toLowerCase());
            if (p == null) throw new UsernameNotFoundException("User not found: " + username);
            return p;
        };
    }

    private TenantPrincipal user(String username, String rawPassword, String tenant, String role, String employeeId, PasswordEncoder encoder) {
        return new TenantPrincipal(username, encoder.encode(rawPassword), tenant, role, employeeId);
    }

    @Bean
    PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }
}
