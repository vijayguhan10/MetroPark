# MetroPark

A **high-performance, horizontally scalable parking reservation platform** built to process **1M+ operations per minute** with low latency, atomic slot allocation, and real-time parking availability.

The system is designed around an **event-driven architecture**, where Redis handles real-time state management while PostgreSQL remains the source of truth.

---

# Tech Stack

| Layer            | Technology    |
| ---------------- | ------------- |
| Language         | Java 21       |
| Framework        | Spring Boot 3 |
| Network Server   | Netty         |
| Database         | PostgreSQL    |
| SQL Layer        | jOOQ DSL      |
| Cache            | Redis Cluster |
| Message Queue    | RabbitMQ      |
| Event Streaming  | Kafka         |
| Containerization | Docker        |
| Build Tool       | Maven         |

---

# Architecture

```text
                     Client
                        │
                        ▼
              Reservation Service
           (Spring Boot + Netty)
                        │
        ┌───────────────┼────────────────┐
        │               │                │
        ▼               ▼                ▼
 Redis Cluster      RabbitMQ         Kafka
(Real-time State) (Async Events) (Streaming/Event Bus)
        │               │
        │               ▼
        │      Background Workers
        │               │
        └───────────────┼───────────────┐
                        ▼               │
                 PostgreSQL             │
            (Source of Truth)           │
```

---

# Key Features

* Atomic slot reservation using Redis
* Prevents double booking under heavy concurrency
* Priority-based waiting queue using Redis Sorted Sets
* Event-driven asynchronous architecture
* Horizontally scalable stateless services
* PostgreSQL as the persistent source of truth
* Dockerized deployment
* Fault-tolerant distributed design
* Low-latency reservation workflow

---

# How It Works

### Reservation

1. User requests a parking slot.
2. Redis performs an atomic availability check.
3. If available, the slot is immediately marked as **RESERVED**.
4. A reservation event is published to RabbitMQ.
5. Background workers persist the reservation into PostgreSQL.

---

### Waiting Queue

If no slots are available:

* Users are added to a Redis Sorted Set.
* Queue priority is calculated using:

  * Membership Tier
  * Premium Upgrade
  * Waiting Time
  * VIP Priority

Whenever a slot becomes available, Redis instantly selects the highest-priority user and allocates the slot.

---

### Checkout

When a vehicle exits:

1. Slot status changes to **AVAILABLE** in Redis.
2. A release event is published.
3. The highest-priority user (if any) receives the slot automatically.
4. PostgreSQL is synchronized asynchronously.

---

# Fault Tolerance

### Redis

* Primary store for live parking inventory
* Automatic failover with replicas
* State can be rebuilt from PostgreSQL events if required

### RabbitMQ

* Buffers asynchronous operations
* Prevents request failures during temporary database outages

### PostgreSQL

* Stores reservations, parking sessions, payments, and audit history
* Remains the permanent source of truth

Even if PostgreSQL becomes unavailable, reservations continue using Redis, and queued events are replayed once the database recovers.

---

# Performance

* Designed for **1M+ operations per minute**
* Stateless services for horizontal scaling
* Redis-based atomic operations for microsecond-level state updates
* Asynchronous persistence reduces database contention
* Event-driven architecture minimizes request latency

---

# Core Modules

* Reservation Service
* Parking Slot Management
* Priority Waiting Queue
* Payment & Billing
* Parking Session Management
* User & Membership Management
* Event Processing Workers

---

# Why This Architecture?

Instead of making PostgreSQL handle every reservation request, the platform performs all real-time allocation in Redis.

This provides:

* Faster reservations
* Better scalability
* Lower database load
* Higher throughput
* Eventual consistency through asynchronous synchronization

The result is a system capable of handling massive concurrent traffic while keeping the user experience fast and responsive.
