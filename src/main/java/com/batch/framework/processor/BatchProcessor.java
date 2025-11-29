package com.batch.framework.processor;

import com.batch.framework.model.Batch;

/**
 * Interface for processing (persisting) batches of DTOs
 * Implementations should handle Jakarta Persistence operations
 * @param <T> The type of DTO being processed
 */
public interface BatchProcessor<T> {
    
    /**
     * Processes a single batch within a transaction
     * @param batch The batch to process
     * @throws BatchProcessingException if processing fails
     */
    void process(Batch<T> batch) throws BatchProcessingException;
    
    /**
     * Called before processing starts for a batch ID
     * Can be used for setup/initialization
     * @param batchId The batch ID about to be processed
     */
    default void beforeBatchId(String batchId) {
        // Optional hook
    }
    
    /**
     * Called after all sub-batches for a batch ID are successfully processed
     * @param batchId The batch ID that completed
     */
    default void afterBatchId(String batchId) {
        // Optional hook
    }
}
