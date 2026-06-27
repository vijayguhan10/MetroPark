error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/UserService.java
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/UserService.java
### com.thoughtworks.qdox.parser.ParseException: syntax error @[3,8]

error in qdox parser
file content:
```java
offset: 48
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/UserService.java
text:
```scala
package com.example.Metropark.service;

public p@@ackage com.parking.service;

import com.parking.dto.SuspensionDto;
import com.parking.dto.UserDto;
import com.parking.repository.SuspensionRepository;
import com.parking.repository.UserRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
public class UserService {

    private final UserRepository userRepository;
    private final SuspensionRepository suspensionRepository;

    public UserService(UserRepository userRepository, SuspensionRepository suspensionRepository) {
        this.userRepository = userRepository;
        this.suspensionRepository = suspensionRepository;
    }

    public Mono<UserDto> getUser(String userId) {
        return userRepository.findById(userId);
    }

    public Flux<UserDto> getAllUsers() {
        return userRepository.findAll();
    }

    @Transactional
    public Mono<Void> suspendUser(String userId, String reason, LocalDateTime until, String adminId) {
        SuspensionDto suspension = new SuspensionDto(
                null, userId, reason, LocalDateTime.now(), until, true, adminId
        );

        // Chain the reactive calls: Update user status THEN create suspension record
        return userRepository.updateStatus(userId, "SUSPENDED")
                .then(suspensionRepository.createSuspension(suspension))
                .then();
    }
} {
    
}

```

```



#### Error stacktrace:

```
com.thoughtworks.qdox.parser.impl.Parser.yyerror(Parser.java:2025)
	com.thoughtworks.qdox.parser.impl.Parser.yyparse(Parser.java:2147)
	com.thoughtworks.qdox.parser.impl.Parser.parse(Parser.java:2006)
	com.thoughtworks.qdox.library.SourceLibrary.parse(SourceLibrary.java:232)
	com.thoughtworks.qdox.library.SourceLibrary.parse(SourceLibrary.java:190)
	com.thoughtworks.qdox.library.SourceLibrary.addSource(SourceLibrary.java:94)
	com.thoughtworks.qdox.library.SourceLibrary.addSource(SourceLibrary.java:89)
	com.thoughtworks.qdox.library.SortedClassLibraryBuilder.addSource(SortedClassLibraryBuilder.java:162)
	com.thoughtworks.qdox.JavaProjectBuilder.addSource(JavaProjectBuilder.java:174)
	scala.meta.internal.mtags.JavaMtags.indexRoot(JavaMtags.scala:49)
	scala.meta.internal.metals.SemanticdbDefinition$.foreachWithReturnMtags(SemanticdbDefinition.scala:99)
	scala.meta.internal.metals.Indexer.indexSourceFile(Indexer.scala:560)
	scala.meta.internal.metals.Indexer.$anonfun$reindexWorkspaceSources$3(Indexer.scala:691)
	scala.meta.internal.metals.Indexer.$anonfun$reindexWorkspaceSources$3$adapted(Indexer.scala:688)
	scala.collection.IterableOnceOps.foreach(IterableOnce.scala:630)
	scala.collection.IterableOnceOps.foreach$(IterableOnce.scala:628)
	scala.collection.AbstractIterator.foreach(Iterator.scala:1313)
	scala.meta.internal.metals.Indexer.reindexWorkspaceSources(Indexer.scala:688)
	scala.meta.internal.metals.MetalsLspService.$anonfun$onChange$2(MetalsLspService.scala:940)
	scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)
	scala.concurrent.Future$.$anonfun$apply$1(Future.scala:691)
	scala.concurrent.impl.Promise$Transformation.run(Promise.scala:500)
	java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	java.base/java.lang.Thread.run(Thread.java:1583)
```
#### Short summary: 

QDox parse error in file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/UserService.java