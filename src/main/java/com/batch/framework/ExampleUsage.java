package com.batch.framework;

import com.batch.framework.cleanup.CleanupStrategy;
import com.batch.framework.coordinator.BatchCoordinator;
import com.batch.framework.dto.PersonDTO;
import com.batch.framework.model.Batch;
import com.batch.framework.model.BatchMessage;
import com.batch.framework.processor.BatchProcessingException;
import com.batch.framework.processor.BatchProcessor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Example usage of the batch processing framework
 * 
 * This demonstrates:
 * - Setting up the coordinator
 * - Producing messages
 * - Processing batches
 * - Handling shutdown
 */
public class ExampleUsage {

    public static void main(String[] args) throws Exception {
        System.out.println("=== Batch Processing Framework Example ===\n");

        // 1. Create the message queue
        BlockingQueue<BatchMessage<PersonDTO>> queue = new LinkedBlockingQueue<>();

        // 2. Create processor and cleanup strategy
        ExampleBatchProcessor processor = new ExampleBatchProcessor();
        ExampleCleanupStrategy cleanupStrategy = new ExampleCleanupStrategy();

        // 3. Build the coordinator
        BatchCoordinator<PersonDTO> coordinator = new BatchCoordinator.Builder<PersonDTO>()
                .queue(queue)
                .processor(processor)
                .cleanupStrategy(cleanupStrategy)
                .maxBatchSize(5000)    // Split into 5000-item batches
                .consumerThreads(1)    // Single consumer
                .workerThreads(4)      // 4 parallel workers
                .build();

        // 4. Start the coordinator
        coordinator.start();
        System.out.println("Coordinator started\n");

        // 5. Start producer thread
        Thread producerThread = new Thread(() -> produceMessages(queue));
        producerThread.start();

        // 6. Wait for producer to finish
        producerThread.join();
        System.out.println("\nProducer finished");

        // 7. Give some time for processing
        Thread.sleep(5000);

        // 8. Graceful shutdown
        System.out.println("\nInitiating shutdown...");
        boolean shutdownSuccess = coordinator.shutdown(30);
        
        if (shutdownSuccess) {
            System.out.println("Shutdown completed successfully");
        } else {
            System.out.println("Shutdown timed out");
        }

        // 9. Print summary
        System.out.println("\n=== Summary ===");
        System.out.println("Batches processed: " + processor.getBatchesProcessed());
        System.out.println("Items processed: " + processor.getItemsProcessed());
    }

    /**
     * Simulates a producer adding messages to the queue
     */
    private static void produceMessages(BlockingQueue<BatchMessage<PersonDTO>> queue) {
        try {
            // Batch 1: 100 messages
            System.out.println("Producing batch-1 (100 messages)...");
            for (int i = 0; i < 100; i++) {
                PersonDTO person = new PersonDTO(
                    (long) i,
                    "First" + i,
                    "Last" + i,
                    "email" + i + "@example.com"
                );
                queue.put(BatchMessage.data("batch-1", person));
            }
            queue.put(BatchMessage.endOfBatch("batch-1"));
            System.out.println("Batch-1 complete");

            // Batch 2: 10,000 messages (will split into 2 sub-batches)
            System.out.println("Producing batch-2 (10,000 messages)...");
            for (int i = 0; i < 10000; i++) {
                PersonDTO person = new PersonDTO(
                    (long) i,
                    "First" + i,
                    "Last" + i,
                    "email" + i + "@example.com"
                );
                queue.put(BatchMessage.data("batch-2", person));
            }
            queue.put(BatchMessage.endOfBatch("batch-2"));
            System.out.println("Batch-2 complete");

            // Batch 3: 7,500 messages (will split into 5000 + 2500)
            System.out.println("Producing batch-3 (7,500 messages)...");
            for (int i = 0; i < 7500; i++) {
                PersonDTO person = new PersonDTO(
                    (long) i,
                    "First" + i,
                    "Last" + i,
                    "email" + i + "@example.com"
                );
                queue.put(BatchMessage.data("batch-3", person));
            }
            queue.put(BatchMessage.endOfBatch("batch-3"));
            System.out.println("Batch-3 complete");

        } catch (InterruptedException e) {
            System.err.println("Producer interrupted");
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Example batch processor implementation
     * In real usage, this would use Jakarta Persistence EntityManager
     */
    private static class ExampleBatchProcessor implements BatchProcessor<PersonDTO> {
        private int batchesProcessed = 0;
        private int itemsProcessed = 0;

        @Override
        public synchronized void process(Batch<PersonDTO> batch) throws BatchProcessingException {
            System.out.println("[Processor] Processing batch: " + batch.getBatchId() + 
                             ", sequence: " + batch.getSequenceNumber() + 
                             ", items: " + batch.size());

            // Simulate database persistence
            try {
                Thread.sleep(100); // Simulate DB latency
                
                // In real implementation:
                // EntityTransaction tx = entityManager.getTransaction();
                // tx.begin();
                // for (PersonDTO dto : batch.getItems()) {
                //     PersonEntity entity = convertToEntity(dto);
                //     entityManager.persist(entity);
                // }
                // entityManager.flush();
                // tx.commit();

                batchesProcessed++;
                itemsProcessed += batch.size();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new BatchProcessingException(
                    batch.getBatchId(),
                    batch.getSequenceNumber(),
                    "Processing interrupted",
                    e
                );
            }

            System.out.println("[Processor] Successfully processed batch: " + batch.getBatchId() + 
                             ", sequence: " + batch.getSequenceNumber());
        }

        @Override
        public void beforeBatchId(String batchId) {
            System.out.println("[Processor] Starting batch ID: " + batchId);
        }

        @Override
        public void afterBatchId(String batchId) {
            System.out.println("[Processor] Completed batch ID: " + batchId);
        }

        public int getBatchesProcessed() {
            return batchesProcessed;
        }

        public int getItemsProcessed() {
            return itemsProcessed;
        }
    }

    /**
     * Example cleanup strategy implementation
     * In real usage, this would clean up partial DB writes
     */
    private static class ExampleCleanupStrategy implements CleanupStrategy {
        @Override
        public void cleanup(String batchId, int failedSequenceNumber) {
            System.out.println("[Cleanup] Cleaning up batch: " + batchId + 
                             ", failed at sequence: " + failedSequenceNumber);
            
            // In real implementation:
            // EntityTransaction tx = entityManager.getTransaction();
            // tx.begin();
            // entityManager.createQuery("DELETE FROM PersonEntity WHERE batchId = :batchId")
            //              .setParameter("batchId", batchId)
            //              .executeUpdate();
            // tx.commit();
        }

        @Override
        public void markComplete(String batchId) {
            System.out.println("[Cleanup] Batch completed successfully: " + batchId);
        }
    }
}
