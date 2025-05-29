package com.exchange.order_completed.common;

import lombok.Getter;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.reactive.ServerHttpRequest;

import java.util.UUID;

@Getter
public class UserInfoHeader {

    private final UUID userId;
    private final String username;
    private final UserRole userRole;

    public UserInfoHeader(ServerHttpRequest request) {
        HttpHeaders headers = request.getHeaders();
        this.userId = UUID.fromString(headers.getFirst("X-USER-ID"));
        this.username = headers.getFirst("X-USERNAME");
        this.userRole = UserRole.valueOf(headers.getFirst("X-USER-ROLE"));
    }
}

