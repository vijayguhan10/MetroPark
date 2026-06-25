error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/config/Jook.java:_empty_/DSL#
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/config/Jook.java
empty definition using pc, found symbol in pc: _empty_/DSL#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 477
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/config/Jook.java
text:
```scala
package com.parking.config;

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
        return D@@SL.using(connectionFactory, SQLDialect.POSTGRES);
    }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: _empty_/DSL#