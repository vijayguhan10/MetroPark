# MetroPark Admin Data Simulator - Backend API Endpoints & Sample Payloads

This document provides all the backend API endpoints and sample JSON payloads needed for the frontend Admin Data Simulator to provision infrastructure data.

---

## Base URL
```
http://localhost:8080/api
```

---

## 1. MASTER DATA (Domain Module)

### 1.1 Location Types
**Endpoint:** `POST /api/location-types`  
**Controller:** `LocationTypeController`  
**DTO:** `LocationTypeDto`

**Sample Payload (4 records):**
```json
[
  {
    "typeName": "PERMANENT_RETAIL"
  },
  {
    "typeName": "TEMPORARY_EVENT"
  },
  {
    "typeName": "AIRPORT_PARKING"
  },
  {
    "typeName": "MALL_PARKING"
  }
]
```

**Response:** `201 Created` with message "Location type created successfully."

---

### 1.2 Vehicle Types
**Endpoint:** `POST /api/vehicle-types`  
**Controller:** `VehicleTypeController`  
**DTO:** `VehicleTypeDto`

**Sample Payload (3 records):**
```json
[
  {
    "typeDisplayName": "Car"
  },
  {
    "typeDisplayName": "Bike"
  },
  {
    "typeDisplayName": "Delivery"
  }
]
```

**Response:** `201 Created` with message "Vehicle type created successfully."

---

### 1.3 Reservation Classes
**Endpoint:** `POST /api/reservation-classes`  
**Controller:** `ReservationClassController`  
**DTO:** `ReservationClassDto`

**Sample Payload (2 records):**
```json
[
  {
    "className": "General"
  },
  {
    "className": "VIP"
  }
]
```

**Response:** `201 Created` with message "Reservation class created successfully."

---

## 2. LOCATIONS & EVENTS (Domain Module)

### 2.1 Create Location
**Endpoint:** `POST /api/locations`  
**Controller:** `LocationController`  
**DTO:** `LocationDto`

**Sample Payload (2 records):**
```json
[
  {
    "locationId": "LOC-CENTRAL-HUB",
    "typeId": 1,
    "locationName": "Central Hub Parking",
    "city": "CityCenter",
    "status": "ACTIVE"
  },
  {
    "locationId": "LOC-EVENT-GROUNDS",
    "typeId": 2,
    "locationName": "Event Grounds Parking",
    "city": "CityCenter",
    "status": "ACTIVE"
  }
]
```

**Notes:**
- `locationId` is a String (UUID or custom ID)
- `typeId` references LocationType (1=PERMANENT_RETAIL, 2=TEMPORARY_EVENT)
- `status` values: `ACTIVE`, `INACTIVE`, `MAINTENANCE`

**Response:** `201 Created` with message "Location created successfully."

---

### 2.2 Update Location Status
**Endpoint:** `PATCH /api/locations/{locationId}/status`  
**Query Param:** `status=ACTIVE|INACTIVE|MAINTENANCE`

---

## 3. GATES INFRASTRUCTURE (Domain Module)

### 3.1 Create Gates
**Endpoint:** `POST /api/gates`  
**Controller:** `GateController`  
**DTO:** `GateDto`

**Sample Payload (4 records - 2 Entry + 2 Exit for Central Hub):**
```json
[
  {
    "locationId": "LOC-CENTRAL-HUB",
    "gateName": "Main Entry Gate A",
    "gateType": "ENTRY",
    "status": "ACTIVE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "gateName": "Main Entry Gate B",
    "gateType": "ENTRY",
    "status": "ACTIVE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "gateName": "Main Exit Gate A",
    "gateType": "EXIT",
    "status": "ACTIVE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "gateName": "Main Exit Gate B",
    "gateType": "EXIT",
    "status": "ACTIVE"
  }
]
```

**Notes:**
- `gateId` is auto-generated (Integer)
- `gateType` values: `ENTRY`, `EXIT`
- `status` values: `ACTIVE`, `INACTIVE`, `MAINTENANCE`
- `createdAt` and `updatedAt` are auto-generated (LocalDateTime)

**Response:** `201 Created` with message "Gate created successfully."

---

### 3.2 Update Gate Status
**Endpoint:** `PATCH /api/gates/{gateId}/status`  
**Query Param:** `status=ACTIVE|INACTIVE|MAINTENANCE`

---

## 4. PARKING INVENTORY (Domain Module)

### 4.1 Create Parking Slots
**Endpoint:** `POST /api/parking-slots`  
**Controller:** `ParkingSlotController`  
**DTO:** `ParkingSlotDto`

**Sample Payload (20 slots distributed across vehicle types and reservation classes):**
```json
[
  {
    "locationId": "LOC-CENTRAL-HUB",
    "displayCode": "A-01",
    "vehicleTypeId": 1,
    "reservationClassId": 1,
    "sensorId": "SENSOR-A01",
    "currentStatus": "AVAILABLE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "displayCode": "A-02",
    "vehicleTypeId": 1,
    "reservationClassId": 1,
    "sensorId": "SENSOR-A02",
    "currentStatus": "AVAILABLE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "displayCode": "A-03",
    "vehicleTypeId": 1,
    "reservationClassId": 1,
    "sensorId": "SENSOR-A03",
    "currentStatus": "AVAILABLE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "displayCode": "A-04",
    "vehicleTypeId": 1,
    "reservationClassId": 1,
    "sensorId": "SENSOR-A04",
    "currentStatus": "AVAILABLE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "displayCode": "A-05",
    "vehicleTypeId": 1,
    "reservationClassId": 1,
    "sensorId": "SENSOR-A05",
    "currentStatus": "AVAILABLE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "displayCode": "B-01",
    "vehicleTypeId": 1,
    "reservationClassId": 2,
    "sensorId": "SENSOR-B01",
    "currentStatus": "AVAILABLE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "displayCode": "B-02",
    "vehicleTypeId": 1,
    "reservationClassId": 2,
    "sensorId": "SENSOR-B02",
    "currentStatus": "AVAILABLE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "displayCode": "B-03",
    "vehicleTypeId": 1,
    "reservationClassId": 2,
    "sensorId": "SENSOR-B03",
    "currentStatus": "AVAILABLE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "displayCode": "B-04",
    "vehicleTypeId": 1,
    "reservationClassId": 2,
    "sensorId": "SENSOR-B04",
    "currentStatus": "AVAILABLE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "displayCode": "B-05",
    "vehicleTypeId": 1,
    "reservationClassId": 2,
    "sensorId": "SENSOR-B05",
    "currentStatus": "AVAILABLE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "displayCode": "C-01",
    "vehicleTypeId": 2,
    "reservationClassId": 1,
    "sensorId": "SENSOR-C01",
    "currentStatus": "AVAILABLE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "displayCode": "C-02",
    "vehicleTypeId": 2,
    "reservationClassId": 1,
    "sensorId": "SENSOR-C02",
    "currentStatus": "AVAILABLE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "displayCode": "C-03",
    "vehicleTypeId": 2,
    "reservationClassId": 1,
    "sensorId": "SENSOR-C03",
    "currentStatus": "AVAILABLE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "displayCode": "C-04",
    "vehicleTypeId": 2,
    "reservationClassId": 1,
    "sensorId": "SENSOR-C04",
    "currentStatus": "AVAILABLE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "displayCode": "C-05",
    "vehicleTypeId": 2,
    "reservationClassId": 1,
    "sensorId": "SENSOR-C05",
    "currentStatus": "AVAILABLE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "displayCode": "D-01",
    "vehicleTypeId": 2,
    "reservationClassId": 2,
    "sensorId": "SENSOR-D01",
    "currentStatus": "AVAILABLE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "displayCode": "D-02",
    "vehicleTypeId": 2,
    "reservationClassId": 2,
    "sensorId": "SENSOR-D02",
    "currentStatus": "AVAILABLE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "displayCode": "D-03",
    "vehicleTypeId": 2,
    "reservationClassId": 2,
    "sensorId": "SENSOR-D03",
    "currentStatus": "AVAILABLE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "displayCode": "E-01",
    "vehicleTypeId": 3,
    "reservationClassId": 1,
    "sensorId": "SENSOR-E01",
    "currentStatus": "AVAILABLE"
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "displayCode": "E-02",
    "vehicleTypeId": 3,
    "reservationClassId": 1,
    "sensorId": "SENSOR-E02",
    "currentStatus": "AVAILABLE"
  }
]
```

**Notes:**
- `slotId` is auto-generated (Integer)
- `vehicleTypeId`: 1=Car, 2=Bike, 3=Delivery
- `reservationClassId`: 1=General, 2=VIP
- `currentStatus` values: `AVAILABLE`, `OCCUPIED`, `RESERVED`, `MAINTENANCE`, `OUT_OF_SERVICE`
- `sensorId` is a mock sensor identifier (e.g., "SENSOR-A01")

**Response:** `201 Created` with message "Parking slot registered successfully."

---

## 5. PAYMENT METHODS (Domain Module)

### 5.1 Create Payment Methods
**Endpoint:** `POST /api/payment-methods`  
**Controller:** `PaymentMethodController`  
**DTO:** `PaymentMethodDto`

**Sample Payload (4 records):**
```json
[
  {
    "methodName": "CASH",
    "isActive": true
  },
  {
    "methodName": "CREDIT_CARD",
    "isActive": true
  },
  {
    "methodName": "UPI",
    "isActive": true
  },
  {
    "methodName": "WALLET",
    "isActive": true
  }
]
```

**Notes:**
- `methodId` is auto-generated (Long)
- `createdAt` is auto-generated (LocalDateTime)
- `isActive` defaults to true if not provided

**Response:** `201 Created` with message "Payment method created successfully."

---

## 6. PRICING RULES (Domain Module)

### 6.1 Create Pricing Rates
**Endpoint:** `POST /api/pricing-rates`  
**Controller:** `PricingRateController`  
**DTO:** `PricingRateDto`

**Sample Payload (4 records - one per vehicle type per location):**
```json
[
  {
    "locationId": "LOC-CENTRAL-HUB",
    "vehicleTypeId": 1,
    "baseRate": 50.00,
    "currency": "INR",
    "effectiveFrom": "2025-01-01T00:00:00",
    "effectiveTo": null,
    "isActive": true
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "vehicleTypeId": 2,
    "baseRate": 20.00,
    "currency": "INR",
    "effectiveFrom": "2025-01-01T00:00:00",
    "effectiveTo": null,
    "isActive": true
  },
  {
    "locationId": "LOC-CENTRAL-HUB",
    "vehicleTypeId": 3,
    "baseRate": 100.00,
    "currency": "INR",
    "effectiveFrom": "2025-01-01T00:00:00",
    "effectiveTo": null,
    "isActive": true
  },
  {
    "locationId": "LOC-EVENT-GROUNDS",
    "vehicleTypeId": 1,
    "baseRate": 100.00,
    "currency": "INR",
    "effectiveFrom": "2025-01-01T00:00:00",
    "effectiveTo": "2025-12-31T23:59:59",
    "isActive": true
  }
]
```

**Notes:**
- `rateId` is auto-generated (Long)
- `baseRate` is BigDecimal (use decimal format)
- `currency` is ISO 4217 3-letter code (e.g., "INR", "USD")
- `effectiveFrom` and `effectiveTo` are ISO 8601 LocalDateTime format
- `isActive` defaults to true
- `createdAt` and `updatedAt` are auto-generated

**Response:** `201 Created` with message "Pricing rate created successfully."

---

### 6.2 Resolve Pricing Rate (Query)
**Endpoint:** `GET /api/pricing-rates/resolve`  
**Query Params:** `locationId`, `vehicleTypeId`, `effectiveAt` (optional)

---

## 7. BULK OPERATIONS - Push All to DB

### Frontend "Push All to DB" Flow

The frontend should call these endpoints in sequence (dependency order):

```javascript
// 1. Master Data (independent)
POST /api/location-types          // 4 records
POST /api/vehicle-types           // 3 records  
POST /api/reservation-classes     // 2 records
POST /api/payment-methods         // 4 records

// 2. Locations (depends on location-types)
POST /api/locations               // 2 records

// 3. Gates (depends on locations)
POST /api/gates                   // 4 records

// 4. Parking Slots (depends on locations, vehicle-types, reservation-classes)
POST /api/parking-slots           // 20 records

// 5. Pricing Rules (depends on locations, vehicle-types)
POST /api/pricing-rates           // 4 records
```

---

## 8. COMPLETE SAMPLE PAYLOADS FOR FRONTEND

### Master Data Payload (All in one object for "View Data Payload")
```json
{
  "locationTypes": [
    { "typeName": "PERMANENT_RETAIL" },
    { "typeName": "TEMPORARY_EVENT" },
    { "typeName": "AIRPORT_PARKING" },
    { "typeName": "MALL_PARKING" }
  ],
  "vehicleTypes": [
    { "typeDisplayName": "Car" },
    { "typeDisplayName": "Bike" },
    { "typeDisplayName": "Delivery" }
  ],
  "reservationClasses": [
    { "className": "General" },
    { "className": "VIP" }
  ],
  "paymentMethods": [
    { "methodName": "CASH", "isActive": true },
    { "methodName": "CREDIT_CARD", "isActive": true },
    { "methodName": "UPI", "isActive": true },
    { "methodName": "WALLET", "isActive": true }
  ]
}
```

### Locations Payload
```json
{
  "locations": [
    {
      "locationId": "LOC-CENTRAL-HUB",
      "typeId": 1,
      "locationName": "Central Hub Parking",
      "city": "CityCenter",
      "status": "ACTIVE"
    },
    {
      "locationId": "LOC-EVENT-GROUNDS",
      "typeId": 2,
      "locationName": "Event Grounds Parking",
      "city": "CityCenter",
      "status": "ACTIVE"
    }
  ]
}
```

### Gates Payload
```json
{
  "gates": [
    {
      "locationId": "LOC-CENTRAL-HUB",
      "gateName": "Main Entry Gate A",
      "gateType": "ENTRY",
      "status": "ACTIVE"
    },
    {
      "locationId": "LOC-CENTRAL-HUB",
      "gateName": "Main Entry Gate B",
      "gateType": "ENTRY",
      "status": "ACTIVE"
    },
    {
      "locationId": "LOC-CENTRAL-HUB",
      "gateName": "Main Exit Gate A",
      "gateType": "EXIT",
      "status": "ACTIVE"
    },
    {
      "locationId": "LOC-CENTRAL-HUB",
      "gateName": "Main Exit Gate B",
      "gateType": "EXIT",
      "status": "ACTIVE"
    }
  ]
}
```

### Parking Slots Payload (20 slots)
```json
{
  "parkingSlots": [
    { "locationId": "LOC-CENTRAL-HUB", "displayCode": "A-01", "vehicleTypeId": 1, "reservationClassId": 1, "sensorId": "SENSOR-A01", "currentStatus": "AVAILABLE" },
    { "locationId": "LOC-CENTRAL-HUB", "displayCode": "A-02", "vehicleTypeId": 1, "reservationClassId": 1, "sensorId": "SENSOR-A02", "currentStatus": "AVAILABLE" },
    { "locationId": "LOC-CENTRAL-HUB", "displayCode": "A-03", "vehicleTypeId": 1, "reservationClassId": 1, "sensorId": "SENSOR-A03", "currentStatus": "AVAILABLE" },
    { "locationId": "LOC-CENTRAL-HUB", "displayCode": "A-04", "vehicleTypeId": 1, "reservationClassId": 1, "sensorId": "SENSOR-A04", "currentStatus": "AVAILABLE" },
    { "locationId": "LOC-CENTRAL-HUB", "displayCode": "A-05", "vehicleTypeId": 1, "reservationClassId": 1, "sensorId": "SENSOR-A05", "currentStatus": "AVAILABLE" },
    { "locationId": "LOC-CENTRAL-HUB", "displayCode": "B-01", "vehicleTypeId": 1, "reservationClassId": 2, "sensorId": "SENSOR-B01", "currentStatus": "AVAILABLE" },
    { "locationId": "LOC-CENTRAL-HUB", "displayCode": "B-02", "vehicleTypeId": 1, "reservationClassId": 2, "sensorId": "SENSOR-B02", "currentStatus": "AVAILABLE" },
    { "locationId": "LOC-CENTRAL-HUB", "displayCode": "B-03", "vehicleTypeId": 1, "reservationClassId": 2, "sensorId": "SENSOR-B03", "currentStatus": "AVAILABLE" },
    { "locationId": "LOC-CENTRAL-HUB", "displayCode": "B-04", "vehicleTypeId": 1, "reservationClassId": 2, "sensorId": "SENSOR-B04", "currentStatus": "AVAILABLE" },
    { "locationId": "LOC-CENTRAL-HUB", "displayCode": "B-05", "vehicleTypeId": 1, "reservationClassId": 2, "sensorId": "SENSOR-B05", "currentStatus": "AVAILABLE" },
    { "locationId": "LOC-CENTRAL-HUB", "displayCode": "C-01", "vehicleTypeId": 2, "reservationClassId": 1, "sensorId": "SENSOR-C01", "currentStatus": "AVAILABLE" },
    { "locationId": "LOC-CENTRAL-HUB", "displayCode": "C-02", "vehicleTypeId": 2, "reservationClassId": 1, "sensorId": "SENSOR-C02", "currentStatus": "AVAILABLE" },
    { "locationId": "LOC-CENTRAL-HUB", "displayCode": "C-03", "vehicleTypeId": 2, "reservationClassId": 1, "sensorId": "SENSOR-C03", "currentStatus": "AVAILABLE" },
    { "locationId": "LOC-CENTRAL-HUB", "displayCode": "C-04", "vehicleTypeId": 2, "reservationClassId": 1, "sensorId": "SENSOR-C04", "currentStatus": "AVAILABLE" },
    { "locationId": "LOC-CENTRAL-HUB", "displayCode": "C-05", "vehicleTypeId": 2, "reservationClassId": 1, "sensorId": "SENSOR-C05", "currentStatus": "AVAILABLE" },
    { "locationId": "LOC-CENTRAL-HUB", "displayCode": "D-01", "vehicleTypeId": 2, "reservationClassId": 2, "sensorId": "SENSOR-D01", "currentStatus": "AVAILABLE" },
    { "locationId": "LOC-CENTRAL-HUB", "displayCode": "D-02", "vehicleTypeId": 2, "reservationClassId": 2, "sensorId": "SENSOR-D02", "currentStatus": "AVAILABLE" },
    { "locationId": "LOC-CENTRAL-HUB", "displayCode": "D-03", "vehicleTypeId": 2, "reservationClassId": 2, "sensorId": "SENSOR-D03", "currentStatus": "AVAILABLE" },
    { "locationId": "LOC-CENTRAL-HUB", "displayCode": "E-01", "vehicleTypeId": 3, "reservationClassId": 1, "sensorId": "SENSOR-E01", "currentStatus": "AVAILABLE" },
    { "locationId": "LOC-CENTRAL-HUB", "displayCode": "E-02", "vehicleTypeId": 3, "reservationClassId": 1, "sensorId": "SENSOR-E02", "currentStatus": "AVAILABLE" }
  ]
}
```

### Pricing Rates Payload
```json
{
  "pricingRates": [
    {
      "locationId": "LOC-CENTRAL-HUB",
      "vehicleTypeId": 1,
      "baseRate": 50.00,
      "currency": "INR",
      "effectiveFrom": "2025-01-01T00:00:00",
      "effectiveTo": null,
      "isActive": true
    },
    {
      "locationId": "LOC-CENTRAL-HUB",
      "vehicleTypeId": 2,
      "baseRate": 20.00,
      "currency": "INR",
      "effectiveFrom": "2025-01-01T00:00:00",
      "effectiveTo": null,
      "isActive": true
    },
    {
      "locationId": "LOC-CENTRAL-HUB",
      "vehicleTypeId": 3,
      "baseRate": 100.00,
      "currency": "INR",
      "effectiveFrom": "2025-01-01T00:00:00",
      "effectiveTo": null,
      "isActive": true
    },
    {
      "locationId": "LOC-EVENT-GROUNDS",
      "vehicleTypeId": 1,
      "baseRate": 100.00,
      "currency": "INR",
      "effectiveFrom": "2025-01-01T00:00:00",
      "effectiveTo": "2025-12-31T23:59:59",
      "isActive": true
    }
  ]
}
```

---

## 9. FRONTEND INTEGRATION EXAMPLE (JavaScript/TypeScript)

```typescript
// API Base URL
const API_BASE = 'http://localhost:8080/api';

// Helper function for POST requests
async function postData(endpoint: string, data: any) {
  const response = await fetch(`${API_BASE}${endpoint}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data)
  });
  
  if (!response.ok) {
    const error = await response.text();
    throw new Error(`HTTP ${response.status}: ${error}`);
  }
  
  return response.text();
}

// Helper for PATCH requests
async function patchData(endpoint: string, params: URLSearchParams) {
  const response = await fetch(`${API_BASE}${endpoint}?${params}`, {
    method: 'PATCH'
  });
  return response.text();
}

// ============ MASTER DATA ============
async function createMasterData() {
  // Location Types
  for (const lt of masterDataPayload.locationTypes) {
    await postData('/location-types', lt);
  }
  
  // Vehicle Types
  for (const vt of masterDataPayload.vehicleTypes) {
    await postData('/vehicle-types', vt);
  }
  
  // Reservation Classes
  for (const rc of masterDataPayload.reservationClasses) {
    await postData('/reservation-classes', rc);
  }
  
  // Payment Methods
  for (const pm of masterDataPayload.paymentMethods) {
    await postData('/payment-methods', pm);
  }
}

// ============ LOCATIONS ============
async function createLocations() {
  for (const loc of locationsPayload.locations) {
    await postData('/locations', loc);
  }
}

// ============ GATES ============
async function createGates() {
  for (const gate of gatesPayload.gates) {
    await postData('/gates', gate);
  }
}

// ============ PARKING SLOTS ============
async function createParkingSlots() {
  for (const slot of parkingSlotsPayload.parkingSlots) {
    await postData('/parking-slots', slot);
  }
}

// ============ PRICING RATES ============
async function createPricingRates() {
  for (const rate of pricingRatesPayload.pricingRates) {
    await postData('/pricing-rates', rate);
  }
}

// ============ MAIN ORCHESTRATOR ============
async function pushAllToDatabase() {
  try {
    console.log('1. Creating Master Data...');
    await createMasterData();
    
    console.log('2. Creating Locations...');
    await createLocations();
    
    console.log('3. Creating Gates...');
    await createGates();
    
    console.log('4. Creating Parking Slots...');
    await createParkingSlots();
    
    console.log('5. Creating Pricing Rates...');
    await createPricingRates();
    
    console.log('✅ All data pushed successfully!');
  } catch (error) {
    console.error('❌ Error pushing data:', error);
    throw error;
  }
}
```

---

## 10. ERROR HANDLING NOTES

### Common HTTP Responses:
- `201 Created` - Success with message body
- `200 OK` - Success for GET/PATCH/PUT
- `400 Bad Request` - Validation error (check request body)
- `404 Not Found` - Resource not found (for GET by ID, PATCH, PUT, DELETE)
- `409 Conflict` - Duplicate entry (e.g., duplicate name)
- `500 Internal Server Error` - Server error

### Validation Errors:
All DTOs use Jakarta Bean Validation annotations. Common validation errors:
- `@NotBlank` - String fields cannot be empty
- `@NotNull` - Required fields
- `@Positive` - Numeric fields must be > 0
- `@Size(min=3, max=3)` - Currency must be 3 characters

---

## 11. DEPENDENCY ORDER SUMMARY

| Order | Module | Endpoint | Depends On |
|-------|--------|----------|------------|
| 1 | Location Types | POST /api/location-types | - |
| 1 | Vehicle Types | POST /api/vehicle-types | - |
| 1 | Reservation Classes | POST /api/reservation-classes | - |
| 1 | Payment Methods | POST /api/payment-methods | - |
| 2 | Locations | POST /api/locations | Location Types |
| 3 | Gates | POST /api/gates | Locations |
| 4 | Parking Slots | POST /api/parking-slots | Locations, Vehicle Types, Reservation Classes |
| 5 | Pricing Rates | POST /api/pricing-rates | Locations, Vehicle Types |

---

## 12. VERIFICATION ENDPOINTS (GET All)

After pushing data, verify with:
- `GET /api/location-types` - List all location types
- `GET /api/vehicle-types` - List all vehicle types
- `GET /api/reservation-classes` - List all reservation classes
- `GET /api/payment-methods` - List all payment methods
- `GET /api/locations` - List all locations
- `GET /api/gates` - List all gates
- `GET /api/parking-slots` - List all parking slots
- `GET /api/pricing-rates` - List all pricing rates