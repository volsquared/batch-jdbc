# Batch Framework - Quick Reference

## Project Structure
```
batch-framework/
├── pom.xml                          # Maven configuration
├── README.md                        # Full documentation
├── .gitignore                       # Git ignore rules
└── src/
    ├── main/java/com/batch/framework/
    │   ├── model/
    │   │   ├── BatchMessage.java           # Message wrapper with batch ID
    │   │   ├── Batch.java                  # Sub-batch container
    │   │   └── BatchAccumulator.java       # Groups & splits messages
    │   ├── processor/
    │   │   ├── BatchProcessor.java         # Interface: implement your persistence
    │   │   └── BatchProcessingException.java
    │   ├── consumer/
    │   │   ├── BatchConsumer.java          # Reads from queue
    │   │   └── BatchWorker.java            # Processes batch IDs
    │   ├── coordinator/
    │   │   └── BatchCoordinator.java       # Orchestrates everything
    │   ├── cleanup/
    │   │   ├── CleanupStrategy.java        # Interface: implement cleanup
    │   │   └── NoOpCleanupStrategy.java    # Default no-op
    │   ├── repository/
    │   │   ├── BatchRepository.java        # Abstract Jakarta repo
    │   │   └── PersonRepository.java       # Example implementation
    │   ├── dto/
    │   │   └── PersonDTO.java              # Example DTO
    │   └── ExampleUsage.java               # Complete example
    └── test/java/com/batch/framework/
        ├── model/
        │   ├── BatchMessageTest.java
        │   └── BatchAccumulatorTest.java
        ├── consumer/
        │   ├── BatchConsumerTest.java
        │   └── BatchWorkerTest.java
        ├── coordinator/
        │   └── BatchCoordinatorTest.java
        └── integration/
            └── BatchFrameworkIntegrationTest.java
```

## Key Configuration Points

### 1. Batch Size (Line to Change)
**File:** When building BatchCoordinator
```java
.maxBatchSize(5000)  // ← CHANGE THIS
```

### 2. Thread Pools
**File:** When building BatchCoordinator
```java
.consumerThreads(1)  // ← Usually keep at 1
.workerThreads(4)    // ← Increase for more parallel batch ID processing
```

### 3. Your DTO
**File:** Replace `src/main/java/com/batch/framework/dto/PersonDTO.java`
- Add your actual DTO fields
- Update equals/hashCode/toString

### 4. Your Processor
**File:** Implement `BatchProcessor<YourDTO>`
```java
public class YourBatchProcessor implements BatchProcessor<YourDTO> {
    @Override
    public void process(Batch<YourDTO> batch) throws BatchProcessingException {
        // Your Jakarta Persistence code here
        EntityTransaction tx = entityManager.getTransaction();
        tx.begin();
        try {
            for (YourDTO dto : batch.getItems()) {
                // Convert to entity and persist
            }
            tx.commit();
        } catch (Exception e) {
            tx.rollback();
            throw new BatchProcessingException(...);
        }
    }
}
```

### 5. Your Cleanup
**File:** Implement `CleanupStrategy`
```java
public class YourCleanupStrategy implements CleanupStrategy {
    @Override
    public void cleanup(String batchId, int failedSequenceNumber) {
        // Delete all entities with this batchId from DB
    }
}
```

## Running the Example

```bash
# Build
mvn clean compile

# Run tests
mvn test

# Run example
mvn exec:java -Dexec.mainClass="com.batch.framework.ExampleUsage"
```

## Message Flow Example

```java
// Producer adds messages
queue.put(BatchMessage.data("order-123", orderDTO1));
queue.put(BatchMessage.data("order-123", orderDTO2));
// ... 10,000 messages ...
queue.put(BatchMessage.endOfBatch("order-123"));  // ← Required!

// Framework automatically:
// 1. Consumer reads from queue
// 2. Accumulator groups by batch ID
// 3. Splits into 2 sub-batches (5000 each)
// 4. Worker processes sub-batch 1 → SUCCESS
// 5. Worker processes sub-batch 2 → SUCCESS
// 6. Cleanup marks batch-123 complete
```

## Failure Scenario

```java
// 10,000 messages for batch-456
// Split into 2 sub-batches

// Sub-batch 1 (seq 1): 5000 items → SUCCESS (committed to DB)
// Sub-batch 2 (seq 2): 5000 items → FAILURE (exception thrown)

// Framework automatically:
// 1. Catches exception from sub-batch 2
// 2. No sub-batch 3 is processed (purged)
// 3. Calls cleanup("batch-456", 2)
// 4. Your cleanup deletes all items for batch-456 (including sub-batch 1)
// 5. Continues with next batch ID
```

## Common Patterns

### Pattern 1: Wrap DTOs with Batch ID
```java
String batchId = UUID.randomUUID().toString();
for (OrderDTO order : orders) {
    queue.put(BatchMessage.data(batchId, order));
}
queue.put(BatchMessage.endOfBatch(batchId));
```

### Pattern 2: Multiple Producers
```java
// Producer 1
for (OrderDTO order : batch1) {
    queue.put(BatchMessage.data("batch-1", order));
}
queue.put(BatchMessage.endOfBatch("batch-1"));

// Producer 2 (can run concurrently)
for (OrderDTO order : batch2) {
    queue.put(BatchMessage.data("batch-2", order));
}
queue.put(BatchMessage.endOfBatch("batch-2"));
```

### Pattern 3: Custom Batch ID Format
```java
String batchId = String.format("USER-%s-%s", 
    userId, 
    LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
);
```

## Testing Your Implementation

1. **Unit test your processor**
```java
@Test
void testYourProcessor() throws Exception {
    List<YourDTO> items = createTestData(100);
    Batch<YourDTO> batch = new Batch<>("test-1", 1, items);
    
    processor.process(batch);
    
    // Verify in DB
    assertEquals(100, countInDB("test-1"));
}
```

2. **Integration test full flow**
```java
@Test
void testEndToEnd() throws Exception {
    // Add messages to queue
    // Verify processing completes
    // Check DB state
}
```

## Performance Tuning

- **Large batches**: Increase `maxBatchSize` to reduce transaction overhead
- **Many batch IDs**: Increase `workerThreads` for parallel processing
- **Queue backlog**: Increase queue capacity or add more consumers
- **DB timeouts**: Reduce `maxBatchSize` or optimize your persistence code

## Troubleshooting

**Queue fills up?**
- Increase `workerThreads`
- Optimize your `process()` implementation
- Reduce `maxBatchSize` if transactions are too slow

**Out of memory?**
- Reduce queue capacity
- Reduce `maxBatchSize`
- Process batches faster (more workers)

**Batches not processing?**
- Check you sent `endOfBatch` message
- Check for exceptions in logs
- Verify coordinator is started

**Cleanup not working?**
- Verify `CleanupStrategy.cleanup()` implementation
- Check DB transaction handling
- Look for rollback exceptions

## Next Steps

1. Replace PersonDTO with your DTO
2. Implement your BatchProcessor
3. Implement your CleanupStrategy
4. Update ExampleUsage.java with your logic
5. Run tests: `mvn test`
6. Deploy!
