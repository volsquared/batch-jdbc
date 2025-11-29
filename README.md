# Batch Processing Framework

A robust, thread-safe framework for batch processing DTOs with Jakarta Persistence support.

## Features

- **Configurable batch sizes** - Split large batches automatically
- **Parallel processing** - Multiple workers process different batch IDs concurrently
- **Failure handling** - Automatic cleanup and purging on errors
- **Graceful shutdown** - Drains queue before terminating
- **Abstraction-based design** - Easy to customize for your use case

## Architecture

```
Producer → LinkedBlockingQueue → Consumer Thread → BatchAccumulator
                                                           ↓
                                                    Worker Threads
                                                           ↓
                                                   BatchProcessor
                                                           ↓
                                                   Jakarta Persistence
```

## Quick Start

### 1. Create your DTO
```java
public class PersonDTO {
    private Long id;
    private String firstName;
    private String lastName;
    private String email;
    // getters/setters...
}
```

### 2. Implement BatchProcessor
```java
public class PersonBatchProcessor implements BatchProcessor<PersonDTO> {
    private final EntityManager entityManager;
    
    @Override
    public void process(Batch<PersonDTO> batch) throws BatchProcessingException {
        EntityTransaction tx = entityManager.getTransaction();
        try {
            tx.begin();
            for (PersonDTO dto : batch.getItems()) {
                PersonEntity entity = convertToEntity(dto);
                entityManager.persist(entity);
            }
            entityManager.flush();
            tx.commit();
        } catch (Exception e) {
            if (tx.isActive()) tx.rollback();
            throw new BatchProcessingException(
                batch.getBatchId(), 
                batch.getSequenceNumber(), 
                "Failed to persist", 
                e
            );
        }
    }
}
```

### 3. Implement CleanupStrategy
```java
public class PersonCleanupStrategy implements CleanupStrategy {
    private final EntityManager entityManager;
    
    @Override
    public void cleanup(String batchId, int failedSequenceNumber) {
        EntityTransaction tx = entityManager.getTransaction();
        try {
            tx.begin();
            entityManager.createQuery(
                "DELETE FROM PersonEntity WHERE batchId = :batchId"
            ).setParameter("batchId", batchId)
             .executeUpdate();
            tx.commit();
        } catch (Exception e) {
            if (tx.isActive()) tx.rollback();
            throw new RuntimeException("Cleanup failed", e);
        }
    }
}
```

### 4. Set up the framework
```java
// Create queue
BlockingQueue<BatchMessage<PersonDTO>> queue = new LinkedBlockingQueue<>();

// Create coordinator
BatchCoordinator<PersonDTO> coordinator = new BatchCoordinator.Builder<PersonDTO>()
    .queue(queue)
    .processor(new PersonBatchProcessor(entityManager))
    .cleanupStrategy(new PersonCleanupStrategy(entityManager))
    .maxBatchSize(5000)  // Split into sub-batches of 5000
    .consumerThreads(1)
    .workerThreads(4)
    .build();

// Start processing
coordinator.start();
```

### 5. Produce messages
```java
// Producer thread
for (PersonDTO person : persons) {
    queue.put(BatchMessage.data("batch-123", person));
}

// Signal end of batch
queue.put(BatchMessage.endOfBatch("batch-123"));
```

### 6. Graceful shutdown
```java
// When done
boolean success = coordinator.shutdown(30); // 30 second timeout
```

## Configuration

### Batch Size
The `maxBatchSize` parameter controls how many items are persisted in a single transaction.

Example: 10,000 messages with maxBatchSize=5000 creates 2 sub-batches of 5000 each.

### Thread Pools
- **consumerThreads**: Number of threads reading from queue (typically 1)
- **workerThreads**: Number of threads processing batch IDs (increase for parallel processing)

### Failure Handling
When a sub-batch fails:
1. Remaining sub-batches for that batch ID are purged
2. CleanupStrategy.cleanup() is called
3. Processing continues with next batch ID

## Testing

Run all tests:
```bash
mvn test
```

Run specific test:
```bash
mvn test -Dtest=BatchFrameworkIntegrationTest
```

## Components

### Core Classes
- `BatchMessage<T>` - Wrapper for DTOs with batch metadata
- `Batch<T>` - Sub-batch containing items to persist
- `BatchAccumulator<T>` - Groups messages by batch ID and splits

### Processing
- `BatchProcessor<T>` - Interface for persistence logic
- `BatchConsumer<T>` - Reads from queue and builds batches
- `BatchWorker<T>` - Processes sub-batches for a batch ID

### Coordination
- `BatchCoordinator<T>` - Orchestrates consumers and workers
- `CleanupStrategy` - Handles rollback of partial writes

### Repository
- `BatchRepository<T,E>` - Abstract Jakarta Persistence repository
- Extend this for your entity type

## Example Scenarios

### Scenario 1: Simple batch (100 messages)
```
Messages: 100
MaxBatchSize: 5000
Result: 1 sub-batch with 100 items
```

### Scenario 2: Split batch (10,000 messages)
```
Messages: 10,000
MaxBatchSize: 5000
Result: 2 sub-batches (5000 + 5000)
```

### Scenario 3: Uneven split (7,500 messages)
```
Messages: 7,500
MaxBatchSize: 5000
Result: 2 sub-batches (5000 + 2500)
```

### Scenario 4: Multiple batch IDs
```
batch-1: 1,000 messages
batch-2: 500 messages  
batch-3: 2,000 messages

All processed in parallel by worker threads
```

### Scenario 5: Failure with cleanup
```
batch-1: 10,000 messages (splits into 2 sub-batches)
Sub-batch 1: Persists successfully (5,000 items in DB)
Sub-batch 2: Fails

Result:
- Sub-batch 2 is purged (never persisted)
- CleanupStrategy removes all 5,000 items from sub-batch 1
- Processing continues with next batch ID
```

## Logging

The framework uses `System.out.println()` for logging. To integrate with SLF4J:

1. Replace all `System.out.println()` with logger calls
2. Add SLF4J dependency to pom.xml

```java
private static final Logger logger = LoggerFactory.getLogger(BatchConsumer.class);
logger.info("[BatchConsumer] Started");
```

## Notes

- All DTOs must be immutable or thread-safe
- Batch IDs should be unique per logical batch
- End-of-batch message is required for each batch ID
- Queue capacity should be sized based on expected throughput

## License

This is example code - customize for your needs.
