package com.batch.framework.consumer;

import com.batch.framework.dto.PersonDTO;
import com.batch.framework.model.BatchAccumulator;
import com.batch.framework.model.BatchMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class BatchConsumerTest {

    private BlockingQueue<BatchMessage<PersonDTO>> queue;
    private BatchAccumulator<PersonDTO> accumulator;
    private List<String> submittedBatchIds;
    private BatchConsumer<PersonDTO> consumer;

    @BeforeEach
    void setUp() {
        queue = new LinkedBlockingQueue<>();
        accumulator = new BatchAccumulator<>(5000);
        submittedBatchIds = new ArrayList<>();
        
        consumer = new BatchConsumer<>(
            queue,
            accumulator,
            submittedBatchIds::add,
            100L // Short timeout for tests
        );
    }

    @Test
    void testConsumesDataMessages() throws Exception {
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();

        // Add data messages
        for (int i = 0; i < 10; i++) {
            queue.put(BatchMessage.data("batch-1", 
                new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com")));
        }

        // Wait for consumption
        Thread.sleep(200);

        assertEquals(10, accumulator.getCurrentSize("batch-1"));

        consumer.shutdown();
        consumerThread.join(1000);
    }

    @Test
    void testSubmitsBatchIdOnEndOfBatch() throws Exception {
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();

        queue.put(BatchMessage.data("batch-1", 
            new PersonDTO(1L, "John", "Doe", "john@example.com")));
        queue.put(BatchMessage.endOfBatch("batch-1"));

        // Wait for processing
        Thread.sleep(300);

        assertTrue(accumulator.isComplete("batch-1"));
        assertEquals(1, submittedBatchIds.size());
        assertEquals("batch-1", submittedBatchIds.get(0));

        consumer.shutdown();
        consumerThread.join(1000);
    }

    @Test
    void testHandlesMultipleBatchIds() throws Exception {
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();

        // Add messages for multiple batches
        queue.put(BatchMessage.data("batch-1", 
            new PersonDTO(1L, "John", "Doe", "john@example.com")));
        queue.put(BatchMessage.data("batch-2", 
            new PersonDTO(2L, "Jane", "Doe", "jane@example.com")));
        queue.put(BatchMessage.endOfBatch("batch-1"));
        queue.put(BatchMessage.endOfBatch("batch-2"));

        // Wait for processing
        Thread.sleep(300);

        assertEquals(2, submittedBatchIds.size());
        assertTrue(submittedBatchIds.contains("batch-1"));
        assertTrue(submittedBatchIds.contains("batch-2"));

        consumer.shutdown();
        consumerThread.join(1000);
    }

    @Test
    void testGracefulShutdownDrainsQueue() throws Exception {
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();

        // Add messages
        for (int i = 0; i < 5; i++) {
            queue.put(BatchMessage.data("batch-1", 
                new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com")));
        }

        consumer.shutdown();
        consumerThread.join(2000);

        // All messages should be consumed
        assertEquals(0, queue.size());
        assertEquals(5, accumulator.getCurrentSize("batch-1"));
    }

    @Test
    void testInterruptionDuringPoll() throws Exception {
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();

        Thread.sleep(100);
        
        consumer.shutdown();
        consumerThread.interrupt();
        consumerThread.join(1000);

        assertFalse(consumer.isRunning());
    }

    @Test
    void testEndOfBatchWithNoDataMessages() throws Exception {
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();

        queue.put(BatchMessage.endOfBatch("batch-1"));

        Thread.sleep(200);

        assertEquals(1, submittedBatchIds.size());
        assertEquals("batch-1", submittedBatchIds.get(0));
        assertTrue(accumulator.isComplete("batch-1"));

        consumer.shutdown();
        consumerThread.join(1000);
    }

    @Test
    void testInterleavedMessages() throws Exception {
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();

        // Interleave messages from two batches
        queue.put(BatchMessage.data("batch-1", new PersonDTO(1L, "A", "A", "a@example.com")));
        queue.put(BatchMessage.data("batch-2", new PersonDTO(2L, "B", "B", "b@example.com")));
        queue.put(BatchMessage.data("batch-1", new PersonDTO(3L, "C", "C", "c@example.com")));
        queue.put(BatchMessage.endOfBatch("batch-1"));
        queue.put(BatchMessage.data("batch-2", new PersonDTO(4L, "D", "D", "d@example.com")));
        queue.put(BatchMessage.endOfBatch("batch-2"));

        Thread.sleep(300);

        assertEquals(2, accumulator.getCurrentSize("batch-1"));
        assertEquals(2, accumulator.getCurrentSize("batch-2"));
        assertEquals(2, submittedBatchIds.size());

        consumer.shutdown();
        consumerThread.join(1000);
    }
}
