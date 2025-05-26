package com.exchange.order_completed.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.data.cassandra.ReactiveSession;

@Configuration
public class CassandraReactiveConfig {

    @Bean
    public ReactiveCqlTemplate reactiveCqlTemplate(ReactiveSession reactiveSession) {
        return new ReactiveCqlTemplate(reactiveSession);
    }
}
