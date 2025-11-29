package com.batch.framework.model;

import com.batch.framework.dto.PersonDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BatchAccumulatorTest {

    private BatchAccumulator<PersonDTO> accumulator;

    @BeforeEach
    void setUp() {
        accumulator = new BatchAccumulator<>(5000);
    }

    @Test
    void testConstructorValidatesMaxBatchSize() {
        assertThrows(IllegalArgumentException.class, () -> 
            new BatchAccumulator<PersonDTO>(0)
        );
        assertThrows(IllegalArgumentException.class, () -> 
            new BatchAccumulator<PersonDTO>(-1)
        );
    }

    @Test
    void testAddDataMessage() {
        PersonDTO person = new PersonDTO(1L, "John", "Doe", "john@example.com");
        BatchMessage<PersonDTO> message = BatchMessage.data("batch-1", person);

        accumulator.add(message);

        assertEquals(1, accumulator.getCurrentSize("batch-1"));
        assertFalse(accumulator.isComplete("batch-1"));
    }

    @Test
    void testAddEndOfBatchMessage() {
        BatchMessage<PersonDTO> endMessage = BatchMessage.endOfBatch("batch-1");

        accumulator.add(endMessage);

        assertTrue(accumulator.isComplete("batch-1"));
        assertEquals(0, accumulator.getCurrentSize("batch-1"));
    }

    @Test
    void testRetrieveAndRemoveWithNoData() {
        BatchMessage<PersonDTO> endMessage = BatchMessage.endOfBatch("batch-1");
        accumulator.add(endMessage);

        List<Batch<PersonDTO>> batches = accumulator.retrieveAndRemove("batch-1");

        assertTrue(batches.isEmpty());
        assertFalse(accumulator.isComplete("batch-1"));
    }

    @Test
    void testRetrieveAndRemoveWithSingleBatch() {
        // Add 100 messages (less than max of 5000)
        for (int i = 0; i < 100; i++) {
            PersonDTO person = new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com");
            accumulator.add(BatchMessage.data("batch-1", person));
        }
        accumulator.add(BatchMessage.endOfBatch("batch-1"));

        List<Batch<PersonDTO>> batches = accumulator.retrieveAndRemove("batch-1");

        assertEquals(1, batches.size());
        assertEquals(100, batches.get(0).size());
        assertEquals("batch-1", batches.get(0).getBatchId());
        assertEquals(1, batches.get(0).getSequenceNumber());
    }

    @Test
    void testRetrieveAndRemoveWithMultipleBatches() {
        // Add 10,000 messages (will split into 2 batches of 5000 each)
        for (int i = 0; i < 10000; i++) {
            PersonDTO person = new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com");
            accumulator.add(BatchMessage.data("batch-1", person));
        }
        accumulator.add(BatchMessage.endOfBatch("batch-1"));

        List<Batch<PersonDTO>> batches = accumulator.retrieveAndRemove("batch-1");

        assertEquals(2, batches.size());
        assertEquals(5000, batches.get(0).size());
        assertEquals(5000, batches.get(1).size());
        assertEquals(1, batches.get(0).getSequenceNumber());
        assertEquals(2, batches.get(1).getSequenceNumber());
    }

    @Test
    void testRetrieveAndRemoveWithUnevenSplit() {
        // Add 7,500 messages (will split into 5000 + 2500)
        for (int i = 0; i < 7500; i++) {
            PersonDTO person = new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com");
            accumulator.add(BatchMessage.data("batch-1", person));
        }
        accumulator.add(BatchMessage.endOfBatch("batch-1"));

        List<Batch<PersonDTO>> batches = accumulator.retrieveAndRemove("batch-1");

        assertEquals(2, batches.size());
        assertEquals(5000, batches.get(0).size());
        assertEquals(2500, batches.get(1).size());
    }

    @Test
    void testRetrieveBeforeEndOfBatchReturnsEmpty() {
        for (int i = 0; i < 100; i++) {
            PersonDTO person = new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com");
            accumulator.add(BatchMessage.data("batch-1", person));
        }

        List<Batch<PersonDTO>> batches = accumulator.retrieveAndRemove("batch-1");

        assertTrue(batches.isEmpty());
    }

    @Test
    void testMultipleBatchIds() {
        // Add to batch-1
        for (int i = 0; i < 50; i++) {
            accumulator.add(BatchMessage.data("batch-1", 
                new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com")));
        }

        // Add to batch-2
        for (int i = 0; i < 75; i++) {
            accumulator.add(BatchMessage.data("batch-2", 
                new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com")));
        }

        assertEquals(50, accumulator.getCurrentSize("batch-1"));
        assertEquals(75, accumulator.getCurrentSize("batch-2"));

        accumulator.add(BatchMessage.endOfBatch("batch-1"));
        assertTrue(accumulator.isComplete("batch-1"));
        assertFalse(accumulator.isComplete("batch-2"));
    }

    @Test
    void testPurge() {
        for (int i = 0; i < 100; i++) {
            accumulator.add(BatchMessage.data("batch-1", 
                new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com")));
        }
        accumulator.add(BatchMessage.endOfBatch("batch-1"));

        accumulator.purge("batch-1");

        assertFalse(accumulator.isComplete("batch-1"));
        assertEquals(0, accumulator.getCurrentSize("batch-1"));
    }

    @Test
    void testGetCompletedBatchIds() {
        accumulator.add(BatchMessage.endOfBatch("batch-1"));
        accumulator.add(BatchMessage.endOfBatch("batch-2"));

        var completed = accumulator.getCompletedBatchIds();

        assertEquals(2, completed.size());
        assertTrue(completed.contains("batch-1"));
        assertTrue(completed.contains("batch-2"));
    }

    @Test
    void testExactlyMaxSizeBatch() {
        // Add exactly 5000 messages
        for (int i = 0; i < 5000; i++) {
            accumulator.add(BatchMessage.data("batch-1", 
                new PersonDTO((long) i, "First" + i, "Last" + i, "email" + i + "@example.com")));
        }
        accumulator.add(BatchMessage.endOfBatch("batch-1"));

        List<Batch<PersonDTO>> batches = accumulator.retrieveAndRemove("batch-1");

        assertEquals(1, batches.size());
        assertEquals(5000, batches.get(0).size());
    }
}
