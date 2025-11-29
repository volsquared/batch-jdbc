package com.batch.framework.model;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Accumulates messages by batch ID and splits into sub-batches when max size is reached
 * Thread-safe for concurrent access
 * @param <T> The type of DTO being accumulated
 */
public class BatchAccumulator<T> {
    private final int maxBatchSize;
    private final Map<String, List<T>> accumulator;
    private final Set<String> completedBatchIds;

    public BatchAccumulator(int maxBatchSize) {
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("Max batch size must be positive");
        }
        this.maxBatchSize = maxBatchSize;
        this.accumulator = new ConcurrentHashMap<>();
        this.completedBatchIds = ConcurrentHashMap.newKeySet();
    }

    /**
     * Adds a message to the accumulator
     * @param message The message to add
     */
    public synchronized void add(BatchMessage<T> message) {
        if (message.isEndOfBatch()) {
            System.out.println("[BatchAccumulator] End-of-batch received for: " + message.getBatchId());
            completedBatchIds.add(message.getBatchId());
            return;
        }

        String batchId = message.getBatchId();
        accumulator.computeIfAbsent(batchId, k -> new CopyOnWriteArrayList<>())
                   .add(message.getPayload());
        
        System.out.println("[BatchAccumulator] Added message to batch: " + batchId + 
                          ", current size: " + accumulator.get(batchId).size());
    }

    /**
     * Checks if a batch ID has been marked as complete
     */
    public boolean isComplete(String batchId) {
        return completedBatchIds.contains(batchId);
    }

    /**
     * Retrieves and removes all sub-batches for a completed batch ID
     * Splits into sub-batches based on maxBatchSize
     * @param batchId The batch ID to retrieve
     * @return List of sub-batches, or empty list if not complete
     */
    public synchronized List<Batch<T>> retrieveAndRemove(String batchId) {
        if (!completedBatchIds.contains(batchId)) {
            return Collections.emptyList();
        }

        List<T> items = accumulator.remove(batchId);
        completedBatchIds.remove(batchId);

        if (items == null || items.isEmpty()) {
            System.out.println("[BatchAccumulator] No items found for batch: " + batchId);
            return Collections.emptyList();
        }

        List<Batch<T>> subBatches = new ArrayList<>();
        int totalItems = items.size();
        int subBatchCount = (int) Math.ceil((double) totalItems / maxBatchSize);

        System.out.println("[BatchAccumulator] Splitting batch " + batchId + 
                          ": " + totalItems + " items into " + subBatchCount + " sub-batches");

        for (int i = 0; i < subBatchCount; i++) {
            int fromIndex = i * maxBatchSize;
            int toIndex = Math.min(fromIndex + maxBatchSize, totalItems);
            List<T> subBatchItems = new ArrayList<>(items.subList(fromIndex, toIndex));
            
            Batch<T> subBatch = new Batch<>(batchId, i + 1, subBatchItems);
            subBatches.add(subBatch);
            
            System.out.println("[BatchAccumulator] Created sub-batch " + (i + 1) + "/" + 
                              subBatchCount + " with " + subBatchItems.size() + " items");
        }

        return subBatches;
    }

    /**
     * Gets all completed batch IDs ready for processing
     */
    public Set<String> getCompletedBatchIds() {
        return new HashSet<>(completedBatchIds);
    }

    /**
     * Gets current size of accumulated items for a batch ID
     */
    public int getCurrentSize(String batchId) {
        List<T> items = accumulator.get(batchId);
        return items != null ? items.size() : 0;
    }

    /**
     * Removes all data for a batch ID (used for cleanup)
     */
    public synchronized void purge(String batchId) {
        accumulator.remove(batchId);
        completedBatchIds.remove(batchId);
        System.out.println("[BatchAccumulator] Purged batch: " + batchId);
    }

    /**
     * Gets count of batches currently being accumulated
     */
    public int getActiveBatchCount() {
        return accumulator.size();
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }
}
