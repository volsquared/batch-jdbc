package com.batch.framework.coordinator;

import com.batch.framework.cleanup.CleanupStrategy;
import com.batch.framework.dto.PersonDTO;
import com.batch.framework.model.Batch;
import com.batch.framework.model.BatchMessage;
import com.batch.framework.processor.BatchProcessingException;
import com.batch.framework.processor.BatchProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BatchCoordinatorTest {

    @Mock
    private BatchProcessor<PersonDTO> processor;

    @Mock
    private CleanupStrategy cleanupStrategy;

    private BlockingQueue<BatchMessage<PersonDTO>> queue;
    private BatchCoordinator<PersonDTO> coordinator;

    @BeforeEach
    void setUp() {
        queue = new LinkedBlockingQueue<>();
    }

    @Test
    void testStartAndStopCoordinator() {
        coordinator = new BatchCoordinator.Builder<PersonDTO>()
            .queue(queue)
            .processor(processor)
            .maxBatchSize(5000)
            .build();

        assertFalse(coordinator.isStarted());

        coordinator.start();
        assertTrue(coordinator.isStarted());

        boolean shutdownSuccess = coordinator.shutdown(5);
        assertTrue(shutdownSuccess);
    }

    @Test
    void testBuilderRequiresQueue() {
        Exception exception = assertThrows(IllegalStateException.class, () -> 
            new BatchCoordinator.Builder<PersonDTO>()
                .processor(processor)
                .build()
        );

        assertTrue(exception.getMessage().contains("Queue must be set"));
    }

    @Test
    void testBuilderRequiresProcessor() {
        Exception exception = assertThrows(IllegalStateException.class, () -> 
            new BatchCoordinator.Builder<PersonDTO>()
                .queue(queue)
                .build()
        );

        assertTrue(exception.getMessage().contains("Processor must be set"));
    }

    @Test
    void testProcessesSingleBatch() throws Exception {
        CountDownLatch processLatch = new CountDownLatch(1);
        
        doAnswer(invocation -> {
            processLatch.countDown();
            return null;
        }).when(processor).process(any(Batch.class));

        coordinator = new BatchCoordinator.Builder<PersonDTO>()
            .queue(queue)
            .processor(processor)
            .maxBatchSize(5000)
            .cleanupStrategy(cleanupStrategy)
            .build();

        coordinator.start();

        // Add messages
        for (int i = 0; i < 100; i++) {
            queue.put(BatchMessage.data("batch-1", 
                new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com")));
        }
        queue.put(BatchMessage.endOfBatch("batch-1"));

        // Wait for processing
        assertTrue(processLatch.await(5, TimeUnit.SECONDS));

        coordinator.shutdown(5);

        verify(processor).process(any(Batch.class));
        verify(cleanupStrategy).markComplete("batch-1");
    }

    @Test
    void testProcessesMultipleBatchIds() throws Exception {
        CountDownLatch processLatch = new CountDownLatch(2); // 2 batches
        
        doAnswer(invocation -> {
            processLatch.countDown();
            return null;
        }).when(processor).process(any(Batch.class));

        coordinator = new BatchCoordinator.Builder<PersonDTO>()
            .queue(queue)
            .processor(processor)
            .maxBatchSize(5000)
            .build();

        coordinator.start();

        // Add batch-1
        for (int i = 0; i < 50; i++) {
            queue.put(BatchMessage.data("batch-1", 
                new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com")));
        }
        queue.put(BatchMessage.endOfBatch("batch-1"));

        // Add batch-2
        for (int i = 0; i < 75; i++) {
            queue.put(BatchMessage.data("batch-2", 
                new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com")));
        }
        queue.put(BatchMessage.endOfBatch("batch-2"));

        // Wait for processing
        assertTrue(processLatch.await(5, TimeUnit.SECONDS));

        coordinator.shutdown(5);

        verify(processor, times(2)).process(any(Batch.class));
    }

    @Test
    void testHandlesProcessingFailure() throws Exception {
        CountDownLatch cleanupLatch = new CountDownLatch(1);
        
        doThrow(new BatchProcessingException("batch-1", 1, "Database error"))
            .when(processor).process(any(Batch.class));
        
        doAnswer(invocation -> {
            cleanupLatch.countDown();
            return null;
        }).when(cleanupStrategy).cleanup(anyString(), anyInt());

        coordinator = new BatchCoordinator.Builder<PersonDTO>()
            .queue(queue)
            .processor(processor)
            .cleanupStrategy(cleanupStrategy)
            .maxBatchSize(5000)
            .build();

        coordinator.start();

        // Add messages
        for (int i = 0; i < 100; i++) {
            queue.put(BatchMessage.data("batch-1", 
                new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com")));
        }
        queue.put(BatchMessage.endOfBatch("batch-1"));

        // Wait for cleanup
        assertTrue(cleanupLatch.await(5, TimeUnit.SECONDS));

        coordinator.shutdown(5);

        verify(cleanupStrategy).cleanup("batch-1", 1);
        verify(cleanupStrategy, never()).markComplete(anyString());
    }

    @Test
    void testGracefulShutdownDrainsQueue() throws Exception {
        CountDownLatch processLatch = new CountDownLatch(1);
        
        doAnswer(invocation -> {
            Thread.sleep(100); // Simulate some processing time
            processLatch.countDown();
            return null;
        }).when(processor).process(any(Batch.class));

        coordinator = new BatchCoordinator.Builder<PersonDTO>()
            .queue(queue)
            .processor(processor)
            .maxBatchSize(5000)
            .build();

        coordinator.start();

        // Add messages
        for (int i = 0; i < 100; i++) {
            queue.put(BatchMessage.data("batch-1", 
                new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com")));
        }
        queue.put(BatchMessage.endOfBatch("batch-1"));

        // Give consumer time to start processing
        Thread.sleep(200);

        // Shutdown
        boolean shutdownSuccess = coordinator.shutdown(10);
        assertTrue(shutdownSuccess);

        // Verify queue was drained
        assertEquals(0, coordinator.getQueueSize());
        verify(processor).process(any(Batch.class));
    }

    @Test
    void testCustomThreadPoolSizes() {
        coordinator = new BatchCoordinator.Builder<PersonDTO>()
            .queue(queue)
            .processor(processor)
            .consumerThreads(2)
            .workerThreads(8)
            .maxBatchSize(1000)
            .build();

        coordinator.start();
        assertTrue(coordinator.isStarted());
        coordinator.shutdown(5);
    }

    @Test
    void testSplitBatchesProcessedSequentially() throws Exception {
        CountDownLatch processLatch = new CountDownLatch(2); // 2 sub-batches
        
        doAnswer(invocation -> {
            processLatch.countDown();
            return null;
        }).when(processor).process(any(Batch.class));

        coordinator = new BatchCoordinator.Builder<PersonDTO>()
            .queue(queue)
            .processor(processor)
            .maxBatchSize(5000)
            .cleanupStrategy(cleanupStrategy)
            .build();

        coordinator.start();

        // Add 10,000 messages (will split into 2 batches)
        for (int i = 0; i < 10000; i++) {
            queue.put(BatchMessage.data("batch-1", 
                new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com")));
        }
        queue.put(BatchMessage.endOfBatch("batch-1"));

        // Wait for processing
        assertTrue(processLatch.await(5, TimeUnit.SECONDS));

        coordinator.shutdown(5);

        // Should process 2 sub-batches
        verify(processor, times(2)).process(any(Batch.class));
        verify(cleanupStrategy).markComplete("batch-1");
    }
}
