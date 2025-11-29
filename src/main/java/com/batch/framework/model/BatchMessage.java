package com.batch.framework.model;

import java.util.Objects;

/**
 * Wrapper for DTO messages with batch metadata
 * @param <T> The type of DTO being batched
 */
public class BatchMessage<T> {
    private final String batchId;
    private final T payload;
    private final boolean endOfBatch;

    private BatchMessage(String batchId, T payload, boolean endOfBatch) {
        this.batchId = Objects.requireNonNull(batchId, "Batch ID cannot be null");
        this.payload = payload;
        this.endOfBatch = endOfBatch;
    }

    /**
     * Creates a data message with payload
     */
    public static <T> BatchMessage<T> data(String batchId, T payload) {
        Objects.requireNonNull(payload, "Payload cannot be null for data message");
        return new BatchMessage<>(batchId, payload, false);
    }

    /**
     * Creates an end-of-batch marker message
     */
    public static <T> BatchMessage<T> endOfBatch(String batchId) {
        return new BatchMessage<>(batchId, null, true);
    }

    public String getBatchId() {
        return batchId;
    }

    public T getPayload() {
        return payload;
    }

    public boolean isEndOfBatch() {
        return endOfBatch;
    }

    public boolean isData() {
        return !endOfBatch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BatchMessage<?> that = (BatchMessage<?>) o;
        return endOfBatch == that.endOfBatch &&
               Objects.equals(batchId, that.batchId) &&
               Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(batchId, payload, endOfBatch);
    }

    @Override
    public String toString() {
        return "BatchMessage{" +
               "batchId='" + batchId + '\'' +
               ", payload=" + payload +
               ", endOfBatch=" + endOfBatch +
               '}';
    }
}
