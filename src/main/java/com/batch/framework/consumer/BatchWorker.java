package com.batch.framework.consumer;

import com.batch.framework.cleanup.CleanupStrategy;
import com.batch.framework.model.Batch;
import com.batch.framework.model.BatchAccumulator;
import com.batch.framework.processor.BatchProcessingException;
import com.batch.framework.processor.BatchProcessor;

import java.util.List;

/**
 * Worker that processes all sub-batches for a single batch ID
 * Runs in a separate thread from the consumer
 * @param <T> The type of DTO being processed
 */
public class BatchWorker<T> implements Runnable {
    
    private final String batchId;
    private final BatchAccumulator<T> accumulator;
    private final BatchProcessor<T> processor;
    private final CleanupStrategy cleanupStrategy;

    public BatchWorker(
            String batchId,
            BatchAccumulator<T> accumulator,
            BatchProcessor<T> processor,
            CleanupStrategy cleanupStrategy) {
        this.batchId = batchId;
        this.accumulator = accumulator;
        this.processor = processor;
        this.cleanupStrategy = cleanupStrategy;
    }

    @Override
    public void run() {
        System.out.println("[BatchWorker] Started processing batch ID: " + batchId + 
                          " - thread: " + Thread.currentThread().getName());

        try {
            // Retrieve all sub-batches for this batch ID
            List<Batch<T>> subBatches = accumulator.retrieveAndRemove(batchId);
            
            if (subBatches.isEmpty()) {
                System.out.println("[BatchWorker] No sub-batches found for batch ID: " + batchId);
                return;
            }

            System.out.println("[BatchWorker] Processing " + subBatches.size() + 
                              " sub-batch(es) for batch ID: " + batchId);

            // Lifecycle hook
            processor.beforeBatchId(batchId);

            // Process each sub-batch sequentially
            for (int i = 0; i < subBatches.size(); i++) {
                Batch<T> subBatch = subBatches.get(i);
                
                try {
                    System.out.println("[BatchWorker] Processing sub-batch " + (i + 1) + "/" + 
                                      subBatches.size() + ": " + subBatch);
                    
                    processor.process(subBatch);
                    
                    System.out.println("[BatchWorker] Successfully processed sub-batch " + 
                                      subBatch.getSequenceNumber() + " of batch ID: " + batchId);
                    
                } catch (BatchProcessingException e) {
                    System.err.println("[BatchWorker] FAILED processing sub-batch " + 
                                      subBatch.getSequenceNumber() + " of batch ID: " + batchId);
                    System.err.println("[BatchWorker] Error: " + e.getMessage());
                    e.printStackTrace();
                    
                    // Purge remaining sub-batches
                    int remainingCount = subBatches.size() - i - 1;
                    if (remainingCount > 0) {
                        System.out.println("[BatchWorker] Purging " + remainingCount + 
                                          " remaining sub-batch(es) for batch ID: " + batchId);
                    }
                    
                    // Trigger cleanup of partial writes
                    System.out.println("[BatchWorker] Triggering cleanup for batch ID: " + batchId);
                    cleanupStrategy.cleanup(batchId, subBatch.getSequenceNumber());
                    
                    // Error logged, continue with next batch ID
                    return;
                }
            }

            // All sub-batches succeeded
            System.out.println("[BatchWorker] Successfully completed all sub-batches for batch ID: " + 
                              batchId);
            
            // Lifecycle hook
            processor.afterBatchId(batchId);
            cleanupStrategy.markComplete(batchId);
            
        } catch (Exception e) {
            System.err.println("[BatchWorker] Unexpected error processing batch ID: " + batchId);
            e.printStackTrace();
            
            // Still trigger cleanup on unexpected errors
            cleanupStrategy.cleanup(batchId, -1);
            
        } finally {
            System.out.println("[BatchWorker] Finished processing batch ID: " + batchId + 
                              " - thread: " + Thread.currentThread().getName());
        }
    }

    public String getBatchId() {
        return batchId;
    }
}
