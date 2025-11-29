package com.batch.framework.cleanup;

/**
 * No-op implementation of CleanupStrategy
 * Use this when cleanup is handled externally or not needed
 */
public class NoOpCleanupStrategy implements CleanupStrategy {
    
    @Override
    public void cleanup(String batchId, int failedSequenceNumber) {
        System.out.println("[NoOpCleanupStrategy] Cleanup requested for batch: " + batchId + 
                          ", failed sequence: " + failedSequenceNumber + " (no-op)");
    }
    
    @Override
    public void markComplete(String batchId) {
        System.out.println("[NoOpCleanupStrategy] Batch completed: " + batchId + " (no-op)");
    }
}
