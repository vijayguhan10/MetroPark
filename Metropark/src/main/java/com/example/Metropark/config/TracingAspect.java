package com.example.Metropark.config;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;
import reactor.core.observability.micrometer.Micrometer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.LinkedHashMap;
import java.util.Map;

@Aspect
@Component
public class TracingAspect {

    private final ObservationRegistry observationRegistry;

    public TracingAspect(ObservationRegistry observationRegistry) {
        this.observationRegistry = observationRegistry;
    }

    @Pointcut("execution(public * com.example.Metropark..service..*(..))")
    public void serviceMethods() {
    }

    @Pointcut("execution(public * com.example.Metropark..repo..*(..))")
    public void repositoryMethods() {
    }

    @Pointcut("execution(public * com.example.Metropark.event.EventPublisher.*(..))")
    public void eventPublisherMethods() {
    }

    @Pointcut("execution(public * com.example.Metropark.event.consumer.EventConsumer.*(..))")
    public void eventConsumerMethods() {
    }

    @Around("serviceMethods()")
    public Object traceServiceMethod(ProceedingJoinPoint joinPoint) throws Throwable {
        String className = declaringClassName(joinPoint);
        String methodName = methodName(joinPoint);
        return trace(joinPoint, className + "." + methodName, tags(
                "component", "service",
                "code.namespace", className,
                "code.function", methodName));
    }

    @Around("repositoryMethods()")
    public Object traceRepositoryMethod(ProceedingJoinPoint joinPoint) throws Throwable {
        String className = declaringClassName(joinPoint);
        String methodName = methodName(joinPoint);
        String table = className.replaceAll("(Repository|Repo)$", "").toLowerCase();
        return trace(joinPoint, "db " + className + "." + methodName, tags(
                "component", "repository",
                "db.system", "postgresql",
                "db.operation", methodName,
                "db.sql.table", table,
                "code.namespace", className,
                "code.function", methodName));
    }

    @Around("eventPublisherMethods()")
    public Object traceEventPublisherMethod(ProceedingJoinPoint joinPoint) throws Throwable {
        String methodName = methodName(joinPoint);
        return trace(joinPoint, "rabbitmq publish " + methodName, tags(
                "component", "messaging",
                "messaging.system", "rabbitmq",
                "messaging.operation", "publish",
                "code.function", methodName));
    }

    @Around("eventConsumerMethods()")
    public Object traceEventConsumerMethod(ProceedingJoinPoint joinPoint) throws Throwable {
        String methodName = methodName(joinPoint);
        return trace(joinPoint, "rabbitmq consume " + methodName, tags(
                "component", "messaging",
                "messaging.system", "rabbitmq",
                "messaging.operation", "process",
                "code.function", methodName));
    }

    private Object trace(ProceedingJoinPoint joinPoint, String name, Map<String, String> tags) throws Throwable {
        Class<?> returnType = ((MethodSignature) joinPoint.getSignature()).getReturnType();

        if (Mono.class.isAssignableFrom(returnType) || Flux.class.isAssignableFrom(returnType)) {
            Object result = joinPoint.proceed();
            if (result instanceof Mono<?> mono) {
                Mono<?> named = mono.name(name);
                for (Map.Entry<String, String> tag : tags.entrySet()) {
                    named = named.tag(tag.getKey(), tag.getValue());
                }
                return named.tap(Micrometer.observation(observationRegistry));
            }
            if (result instanceof Flux<?> flux) {
                Flux<?> named = flux.name(name);
                for (Map.Entry<String, String> tag : tags.entrySet()) {
                    named = named.tag(tag.getKey(), tag.getValue());
                }
                return named.tap(Micrometer.observation(observationRegistry));
            }
            return result; // declared reactive but returned null
        }

        Observation observation = Observation.createNotStarted(name, observationRegistry);
        tags.forEach(observation::lowCardinalityKeyValue);
        observation.start();
        try (Observation.Scope ignored = observation.openScope()) {
            return joinPoint.proceed();
        } catch (Throwable throwable) {
            observation.error(throwable);
            throw throwable;
        } finally {
            observation.stop();
        }
    }

    private static Map<String, String> tags(String... keyValues) {
        Map<String, String> tags = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            tags.put(keyValues[i], keyValues[i + 1]);
        }
        return tags;
    }

    private static String declaringClassName(ProceedingJoinPoint joinPoint) {
        return joinPoint.getSignature().getDeclaringType().getSimpleName();
    }

    private static String methodName(ProceedingJoinPoint joinPoint) {
        return joinPoint.getSignature().getName();
    }
}
