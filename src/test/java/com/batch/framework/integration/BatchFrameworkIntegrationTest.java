package com.batch.framework.integration;

import com.batch.framework.cleanup.CleanupStrategy;
import com.batch.framework.coordinator.BatchCoordinator;
import com.batch.framework.dto.PersonDTO;
import com.batch.framework.model.Batch;
import com.batch.framework.model.BatchMessage;
import com.batch.framework.processor.BatchProcessingException;
import com.batch.framework.processor.BatchProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the complete batch processing framework
 */
class BatchFrameworkIntegrationTest {

    private BlockingQueue<BatchMessage<PersonDTO>> queue;
    private BatchCoordinator<PersonDTO> coordinator;
    private TestBatchProcessor processor;
    private TestCleanupStrategy cleanupStrategy;

    @BeforeEach
    void setUp() {
        queue = new LinkedBlockingQueue<>();
        processor = new TestBatchProcessor();
        cleanupStrategy = new TestCleanupStrategy();
    }

    @AfterEach
    void tearDown() {
        if (coordinator != null) {
            coordinator.shutdown(10);
        }
    }

    @Test
    void testHappyPath_SingleBatch() throws Exception {
        coordinator = new BatchCoordinator.Builder<PersonDTO>()
            .queue(queue)
            .processor(processor)
            .cleanupStrategy(cleanupStrategy)
            .maxBatchSize(5000)
            .build();

        coordinator.start();

        // Add 100 messages
        for (int i = 0; i < 100; i++) {
            queue.put(BatchMessage.data("batch-1", 
                new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com")));
        }
        queue.put(BatchMessage.endOfBatch("batch-1"));

        // Wait for processing
        assertTrue(processor.awaitCompletion(5, TimeUnit.SECONDS));

        // Verify
        assertEquals(1, processor.processedBatches.size());
        assertEquals(100, processor.totalItemsProcessed.get());
        assertEquals(1, cleanupStrategy.completedBatchIds.size());
        assertEquals(0, cleanupStrategy.cleanedUpBatchIds.size());
    }

    @Test
    void testSplitBatches_10KMessages() throws Exception {
        coordinator = new BatchCoordinator.Builder<PersonDTO>()
            .queue(queue)
            .processor(processor)
            .cleanupStrategy(cleanupStrategy)
            .maxBatchSize(5000)
            .build();

        coordinator.start();

        // Add 10,000 messages (will split into 2 sub-batches)
        for (int i = 0; i < 10000; i++) {
            queue.put(BatchMessage.data("batch-1", 
                new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com")));
        }
        queue.put(BatchMessage.endOfBatch("batch-1"));

        // Wait for processing
        assertTrue(processor.awaitCompletion(5, TimeUnit.SECONDS));

        // Verify
        assertEquals(2, processor.processedBatches.size());
        assertEquals(10000, processor.totalItemsProcessed.get());
        
        // Verify sequence numbers
        assertTrue(processor.processedBatches.stream().anyMatch(b -> b.getSequenceNumber() == 1));
        assertTrue(processor.processedBatches.stream().anyMatch(b -> b.getSequenceNumber() == 2));
    }

    @Test
    void testMultipleBatchIds_Parallel() throws Exception {
        coordinator = new BatchCoordinator.Builder<PersonDTO>()
            .queue(queue)
            .processor(processor)
            .cleanupStrategy(cleanupStrategy)
            .maxBatchSize(5000)
            .workerThreads(4)
            .build();

        coordinator.start();

        // Add interleaved messages for 3 different batch IDs
        for (int batchNum = 1; batchNum <= 3; batchNum++) {
            for (int i = 0; i < 100; i++) {
                queue.put(BatchMessage.data("batch-" + batchNum, 
                    new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com")));
            }
        }

        // Send end-of-batch for all
        queue.put(BatchMessage.endOfBatch("batch-1"));
        queue.put(BatchMessage.endOfBatch("batch-2"));
        queue.put(BatchMessage.endOfBatch("batch-3"));

        // Wait for processing
        processor.setExpectedBatches(3);
        assertTrue(processor.awaitCompletion(5, TimeUnit.SECONDS));

        // Verify
        assertEquals(3, processor.processedBatches.size());
        assertEquals(300, processor.totalItemsProcessed.get());
        assertEquals(3, cleanupStrategy.completedBatchIds.size());
    }

    @Test
    void testFailureScenario_PurgesRemainingBatches() throws Exception {
        processor.setFailOnBatch("batch-1", 1); // Fail on first sub-batch

        coordinator = new BatchCoordinator.Builder<PersonDTO>()
            .queue(queue)
            .processor(processor)
            .cleanupStrategy(cleanupStrategy)
            .maxBatchSize(5000)
            .build();

        coordinator.start();

        // Add 10,000 messages (will split into 2 sub-batches)
        for (int i = 0; i < 10000; i++) {
            queue.put(BatchMessage.data("batch-1", 
                new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com")));
        }
        queue.put(BatchMessage.endOfBatch("batch-1"));

        // Wait for processing/failure
        Thread.sleep(2000);

        // Verify - should only process first batch, then fail
        assertEquals(1, processor.processedBatches.size());
        assertEquals(5000, processor.totalItemsProcessed.get());
        
        // Cleanup should be triggered
        assertEquals(1, cleanupStrategy.cleanedUpBatchIds.size());
        assertTrue(cleanupStrategy.cleanedUpBatchIds.contains("batch-1"));
        assertEquals(0, cleanupStrategy.completedBatchIds.size());
    }

    @Test
    void testFailureScenario_ContinuesWithNextBatchId() throws Exception {
        processor.setFailOnBatch("batch-1", 1); // Fail batch-1

        coordinator = new BatchCoordinator.Builder<PersonDTO>()
            .queue(queue)
            .processor(processor)
            .cleanupStrategy(cleanupStrategy)
            .maxBatchSize(5000)
            .build();

        coordinator.start();

        // Add batch-1 (will fail)
        for (int i = 0; i < 100; i++) {
            queue.put(BatchMessage.data("batch-1", 
                new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com")));
        }
        queue.put(BatchMessage.endOfBatch("batch-1"));

        // Add batch-2 (will succeed)
        for (int i = 0; i < 100; i++) {
            queue.put(BatchMessage.data("batch-2", 
                new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com")));
        }
        queue.put(BatchMessage.endOfBatch("batch-2"));

        // Wait for processing
        Thread.sleep(2000);

        // Verify - batch-1 processed (but failed), batch-2 processed successfully
        assertTrue(processor.processedBatches.size() >= 1);
        
        // Batch-2 should complete successfully
        assertTrue(cleanupStrategy.completedBatchIds.contains("batch-2"));
        assertTrue(cleanupStrategy.cleanedUpBatchIds.contains("batch-1"));
    }

    @Test
    void testGracefulShutdown() throws Exception {
        coordinator = new BatchCoordinator.Builder<PersonDTO>()
            .queue(queue)
            .processor(processor)
            .cleanupStrategy(cleanupStrategy)
            .maxBatchSize(5000)
            .build();

        coordinator.start();

        // Add messages
        for (int i = 0; i < 1000; i++) {
            queue.put(BatchMessage.data("batch-1", 
                new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com")));
        }
        queue.put(BatchMessage.endOfBatch("batch-1"));

        // Initiate shutdown
        boolean shutdownSuccess = coordinator.shutdown(10);

        assertTrue(shutdownSuccess);
        assertEquals(0, coordinator.getQueueSize());
    }

    /**
     * Test batch processor implementation
     */
    private static class TestBatchProcessor implements BatchProcessor<PersonDTO> {
        final List<Batch<PersonDTO>> processedBatches = new CopyOnWriteArrayList<>();
        final AtomicInteger totalItemsProcessed = new AtomicInteger(0);
        final CountDownLatch completionLatch;
        
        private String failBatchId = null;
        private int failSequenceNumber = -1;
        private int expectedBatches = 1;

        public TestBatchProcessor() {
            this.completionLatch = new CountDownLatch(1);
        }

        public void setExpectedBatches(int count) {
            this.expectedBatches = count;
        }

        public void setFailOnBatch(String batchId, int sequenceNumber) {
            this.failBatchId = batchId;
            this.failSequenceNumber = sequenceNumber;
        }

        @Override
        public void process(Batch<PersonDTO> batch) throws BatchProcessingException {
            System.out.println("[TestProcessor] Processing: " + batch);
            
            if (failBatchId != null && 
                batch.getBatchId().equals(failBatchId) && 
                batch.getSequenceNumber() == failSequenceNumber) {
                System.out.println("[TestProcessor] Simulating failure for: " + batch);
                throw new BatchProcessingException(batch.getBatchId(), batch.getSequenceNumber(), 
                    "Simulated failure");
            }

            processedBatches.add(batch);
            totalItemsProcessed.addAndGet(batch.size());
            
            if (processedBatches.size() >= expectedBatches) {
                completionLatch.countDown();
            }
        }

        public boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
            return completionLatch.await(timeout, unit);
        }
    }

    /**
     * Test cleanup strategy implementation
     */
    private static class TestCleanupStrategy implements CleanupStrategy {
        final List<String> cleanedUpBatchIds = new CopyOnWriteArrayList<>();
        final List<String> completedBatchIds = new CopyOnWriteArrayList<>();

        @Override
        public void cleanup(String batchId, int failedSequenceNumber) {
            System.out.println("[TestCleanup] Cleanup: " + batchId + ", failed seq: " + failedSequenceNumber);
            cleanedUpBatchIds.add(batchId);
        }

        @Override
        public void markComplete(String batchId) {
            System.out.println("[TestCleanup] Completed: " + batchId);
            completedBatchIds.add(batchId);
        }
    }
}
