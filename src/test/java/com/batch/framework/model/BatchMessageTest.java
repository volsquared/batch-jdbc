package com.batch.framework.model;

import com.batch.framework.dto.PersonDTO;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BatchMessageTest {

    @Test
    void testDataMessageCreation() {
        PersonDTO person = new PersonDTO(1L, "John", "Doe", "john@example.com");
        BatchMessage<PersonDTO> message = BatchMessage.data("batch-1", person);

        assertEquals("batch-1", message.getBatchId());
        assertEquals(person, message.getPayload());
        assertFalse(message.isEndOfBatch());
        assertTrue(message.isData());
    }

    @Test
    void testEndOfBatchMessageCreation() {
        BatchMessage<PersonDTO> message = BatchMessage.endOfBatch("batch-1");

        assertEquals("batch-1", message.getBatchId());
        assertNull(message.getPayload());
        assertTrue(message.isEndOfBatch());
        assertFalse(message.isData());
    }

    @Test
    void testNullBatchIdThrowsException() {
        PersonDTO person = new PersonDTO(1L, "John", "Doe", "john@example.com");
        
        assertThrows(NullPointerException.class, () -> 
            BatchMessage.data(null, person)
        );
    }

    @Test
    void testNullPayloadForDataMessageThrowsException() {
        assertThrows(NullPointerException.class, () -> 
            BatchMessage.data("batch-1", null)
        );
    }

    @Test
    void testEndOfBatchAllowsNullPayload() {
        BatchMessage<PersonDTO> message = BatchMessage.endOfBatch("batch-1");
        assertNull(message.getPayload());
    }

    @Test
    void testEquality() {
        PersonDTO person1 = new PersonDTO(1L, "John", "Doe", "john@example.com");
        PersonDTO person2 = new PersonDTO(1L, "John", "Doe", "john@example.com");

        BatchMessage<PersonDTO> msg1 = BatchMessage.data("batch-1", person1);
        BatchMessage<PersonDTO> msg2 = BatchMessage.data("batch-1", person2);

        assertEquals(msg1, msg2);
        assertEquals(msg1.hashCode(), msg2.hashCode());
    }

    @Test
    void testToString() {
        PersonDTO person = new PersonDTO(1L, "John", "Doe", "john@example.com");
        BatchMessage<PersonDTO> message = BatchMessage.data("batch-1", person);

        String result = message.toString();
        assertTrue(result.contains("batch-1"));
        assertTrue(result.contains("PersonDTO"));
    }
}
