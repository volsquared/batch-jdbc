package com.batch.framework.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a sub-batch of DTOs for a given batch ID
 * @param <T> The type of DTO in the batch
 */
public class Batch<T> {
    private final String batchId;
    private final int sequenceNumber; // 1, 2, 3... for split batches
    private final List<T> items;

    public Batch(String batchId, int sequenceNumber, List<T> items) {
        this.batchId = Objects.requireNonNull(batchId, "Batch ID cannot be null");
        this.sequenceNumber = sequenceNumber;
        this.items = new ArrayList<>(Objects.requireNonNull(items, "Items cannot be null"));
    }

    public String getBatchId() {
        return batchId;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public List<T> getItems() {
        return Collections.unmodifiableList(items);
    }

    public int size() {
        return items.size();
    }

    public boolean isEmpty() {
        return items.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Batch<?> batch = (Batch<?>) o;
        return sequenceNumber == batch.sequenceNumber &&
               Objects.equals(batchId, batch.batchId) &&
               Objects.equals(items, batch.items);
    }

    @Override
    public int hashCode() {
        return Objects.hash(batchId, sequenceNumber, items);
    }

    @Override
    public String toString() {
        return "Batch{" +
               "batchId='" + batchId + '\'' +
               ", sequenceNumber=" + sequenceNumber +
               ", itemCount=" + items.size() +
               '}';
    }
}
