package com.batch.framework.consumer;

import com.batch.framework.cleanup.CleanupStrategy;
import com.batch.framework.dto.PersonDTO;
import com.batch.framework.model.Batch;
import com.batch.framework.model.BatchAccumulator;
import com.batch.framework.model.BatchMessage;
import com.batch.framework.processor.BatchProcessingException;
import com.batch.framework.processor.BatchProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BatchWorkerTest {

    @Mock
    private BatchProcessor<PersonDTO> processor;

    @Mock
    private CleanupStrategy cleanupStrategy;

    private BatchAccumulator<PersonDTO> accumulator;

    @BeforeEach
    void setUp() {
        accumulator = new BatchAccumulator<>(5000);
    }

    @Test
    void testSuccessfulProcessingOfSingleBatch() throws Exception {
        // Setup
        addMessagesAndComplete("batch-1", 100);

        BatchWorker<PersonDTO> worker = new BatchWorker<>("batch-1", accumulator, processor, cleanupStrategy);

        // Execute
        worker.run();

        // Verify
        verify(processor).beforeBatchId("batch-1");
        verify(processor, times(1)).process(any(Batch.class));
        verify(processor).afterBatchId("batch-1");
        verify(cleanupStrategy).markComplete("batch-1");
        verify(cleanupStrategy, never()).cleanup(anyString(), anyInt());
    }

    @Test
    void testSuccessfulProcessingOfMultipleBatches() throws Exception {
        // Setup - 10,000 messages will create 2 sub-batches
        addMessagesAndComplete("batch-1", 10000);

        BatchWorker<PersonDTO> worker = new BatchWorker<>("batch-1", accumulator, processor, cleanupStrategy);

        // Execute
        worker.run();

        // Verify - should process 2 sub-batches
        verify(processor).beforeBatchId("batch-1");
        verify(processor, times(2)).process(any(Batch.class));
        verify(processor).afterBatchId("batch-1");
        verify(cleanupStrategy).markComplete("batch-1");
        verify(cleanupStrategy, never()).cleanup(anyString(), anyInt());
    }

    @Test
    void testFailureOnFirstBatchTriggersCleanup() throws Exception {
        // Setup
        addMessagesAndComplete("batch-1", 10000);

        // First batch fails
        doThrow(new BatchProcessingException("batch-1", 1, "Database error"))
            .when(processor).process(any(Batch.class));

        BatchWorker<PersonDTO> worker = new BatchWorker<>("batch-1", accumulator, processor, cleanupStrategy);

        // Execute
        worker.run();

        // Verify - should only process first batch, then cleanup
        verify(processor).beforeBatchId("batch-1");
        verify(processor, times(1)).process(any(Batch.class)); // Only first batch attempted
        verify(processor, never()).afterBatchId(anyString()); // Never completes
        verify(cleanupStrategy).cleanup("batch-1", 1);
        verify(cleanupStrategy, never()).markComplete(anyString());
    }

    @Test
    void testFailureOnSecondBatchTriggersCleanup() throws Exception {
        // Setup
        addMessagesAndComplete("batch-1", 10000);

        // Second batch fails
        doNothing().when(processor).process(argThat(batch -> batch.getSequenceNumber() == 1));
        doThrow(new BatchProcessingException("batch-1", 2, "Database error"))
            .when(processor).process(argThat(batch -> batch.getSequenceNumber() == 2));

        BatchWorker<PersonDTO> worker = new BatchWorker<>("batch-1", accumulator, processor, cleanupStrategy);

        // Execute
        worker.run();

        // Verify - should process first batch successfully, fail on second
        verify(processor).beforeBatchId("batch-1");
        verify(processor, times(2)).process(any(Batch.class));
        verify(processor, never()).afterBatchId(anyString());
        verify(cleanupStrategy).cleanup("batch-1", 2);
        verify(cleanupStrategy, never()).markComplete(anyString());
    }

    @Test
    void testEmptyBatchIdDoesNothing() throws Exception {
        // No messages added, just mark complete
        accumulator.add(BatchMessage.endOfBatch("batch-1"));

        BatchWorker<PersonDTO> worker = new BatchWorker<>("batch-1", accumulator, processor, cleanupStrategy);

        // Execute
        worker.run();

        // Verify - no processing should occur
        verify(processor, never()).beforeBatchId(anyString());
        verify(processor, never()).process(any(Batch.class));
        verify(processor, never()).afterBatchId(anyString());
        verify(cleanupStrategy, never()).cleanup(anyString(), anyInt());
        verify(cleanupStrategy, never()).markComplete(anyString());
    }

    @Test
    void testUnexpectedExceptionTriggersCleanup() throws Exception {
        // Setup
        addMessagesAndComplete("batch-1", 100);

        // Unexpected runtime exception
        doThrow(new RuntimeException("Unexpected error"))
            .when(processor).process(any(Batch.class));

        BatchWorker<PersonDTO> worker = new BatchWorker<>("batch-1", accumulator, processor, cleanupStrategy);

        // Execute
        worker.run();

        // Verify - should trigger cleanup even for unexpected exceptions
        verify(cleanupStrategy).cleanup("batch-1", -1);
        verify(cleanupStrategy, never()).markComplete(anyString());
    }

    @Test
    void testProcessorHooksAreCalled() throws Exception {
        // Setup
        addMessagesAndComplete("batch-1", 100);

        BatchWorker<PersonDTO> worker = new BatchWorker<>("batch-1", accumulator, processor, cleanupStrategy);

        // Execute
        worker.run();

        // Verify hooks are called in correct order
        var inOrder = inOrder(processor, cleanupStrategy);
        inOrder.verify(processor).beforeBatchId("batch-1");
        inOrder.verify(processor).process(any(Batch.class));
        inOrder.verify(processor).afterBatchId("batch-1");
        inOrder.verify(cleanupStrategy).markComplete("batch-1");
    }

    @Test
    void testBatchIdWithExactlyMaxSize() throws Exception {
        // Setup - exactly 5000 messages (no split)
        addMessagesAndComplete("batch-1", 5000);

        BatchWorker<PersonDTO> worker = new BatchWorker<>("batch-1", accumulator, processor, cleanupStrategy);

        // Execute
        worker.run();

        // Verify - should process exactly 1 batch
        verify(processor, times(1)).process(any(Batch.class));
        verify(cleanupStrategy).markComplete("batch-1");
    }

    private void addMessagesAndComplete(String batchId, int count) {
        for (int i = 0; i < count; i++) {
            PersonDTO person = new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com");
            accumulator.add(BatchMessage.data(batchId, person));
        }
        accumulator.add(BatchMessage.endOfBatch(batchId));
    }
}
