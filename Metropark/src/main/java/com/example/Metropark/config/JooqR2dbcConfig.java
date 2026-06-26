package com.example.Metropark.config;

import io.r2dbc.spi.ConnectionFactory;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JooqR2dbcConfig {

    @Bean
    public DSLContext dslContext(ConnectionFactory connectionFactory) {
        // Initializes jOOQ to execute queries reactively via R2DBC
        return DSL.using(connectionFactory, SQLDialect.POSTGRES);
    }
}