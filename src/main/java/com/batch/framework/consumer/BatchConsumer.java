package com.batch.framework.consumer;

import com.batch.framework.model.Batch;
import com.batch.framework.model.BatchAccumulator;
import com.batch.framework.model.BatchMessage;

import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Consumer that drains the blocking queue and builds batches
 * When end-of-batch is received, submits the batch ID for processing
 * @param <T> The type of DTO being consumed
 */
public class BatchConsumer<T> implements Runnable {
    
    private final BlockingQueue<BatchMessage<T>> queue;
    private final BatchAccumulator<T> accumulator;
    private final Consumer<String> batchIdSubmitter; // Callback to submit completed batch IDs
    private final long pollTimeoutMillis;
    private volatile boolean running;
    private volatile boolean shutdownRequested;

    public BatchConsumer(
            BlockingQueue<BatchMessage<T>> queue,
            BatchAccumulator<T> accumulator,
            Consumer<String> batchIdSubmitter,
            long pollTimeoutMillis) {
        this.queue = queue;
        this.accumulator = accumulator;
        this.batchIdSubmitter = batchIdSubmitter;
        this.pollTimeoutMillis = pollTimeoutMillis;
        this.running = false;
        this.shutdownRequested = false;
    }

    @Override
    public void run() {
        running = true;
        System.out.println("[BatchConsumer] Started - thread: " + Thread.currentThread().getName());

        try {
            while (!shutdownRequested || !queue.isEmpty()) {
                try {
                    BatchMessage<T> message = queue.poll(pollTimeoutMillis, TimeUnit.MILLISECONDS);
                    
                    if (message == null) {
                        // Timeout - check shutdown flag
                        if (shutdownRequested && queue.isEmpty()) {
                            System.out.println("[BatchConsumer] Queue drained, shutting down gracefully");
                            break;
                        }
                        continue;
                    }

                    System.out.println("[BatchConsumer] Received message: " + message);
                    
                    accumulator.add(message);
                    
                    if (message.isEndOfBatch()) {
                        String batchId = message.getBatchId();
                        System.out.println("[BatchConsumer] End-of-batch detected for: " + batchId + 
                                          ", submitting for processing");
                        batchIdSubmitter.accept(batchId);
                    }
                    
                } catch (InterruptedException e) {
                    System.out.println("[BatchConsumer] Interrupted, checking shutdown flag");
                    Thread.currentThread().interrupt();
                    if (shutdownRequested) {
                        break;
                    }
                }
            }
            
        } finally {
            running = false;
            System.out.println("[BatchConsumer] Stopped - thread: " + Thread.currentThread().getName());
        }
    }

    /**
     * Initiates graceful shutdown
     */
    public void shutdown() {
        System.out.println("[BatchConsumer] Shutdown requested");
        shutdownRequested = true;
    }

    public boolean isRunning() {
        return running;
    }

    public boolean isShutdownRequested() {
        return shutdownRequested;
    }

    /**
     * Gets count of messages currently in queue
     */
    public int getQueueSize() {
        return queue.size();
    }
}
