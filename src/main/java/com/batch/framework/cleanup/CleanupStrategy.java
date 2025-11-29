package com.batch.framework.cleanup;

/**
 * Strategy for cleaning up partial writes when batch processing fails
 */
public interface CleanupStrategy {
    
    /**
     * Cleans up partial writes for a failed batch ID
     * @param batchId The batch ID that failed
     * @param failedSequenceNumber The sequence number that failed (sub-batches 1..N may have succeeded)
     */
    void cleanup(String batchId, int failedSequenceNumber);
    
    /**
     * Marks a batch ID as successfully completed (no cleanup needed)
     * @param batchId The batch ID that completed successfully
     */
    default void markComplete(String batchId) {
        // Optional hook for tracking successful batches
    }
}
