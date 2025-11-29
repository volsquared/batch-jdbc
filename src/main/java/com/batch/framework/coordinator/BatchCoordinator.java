package com.batch.framework.coordinator;

import com.batch.framework.cleanup.CleanupStrategy;
import com.batch.framework.cleanup.NoOpCleanupStrategy;
import com.batch.framework.consumer.BatchConsumer;
import com.batch.framework.consumer.BatchWorker;
import com.batch.framework.model.BatchAccumulator;
import com.batch.framework.model.BatchMessage;
import com.batch.framework.processor.BatchProcessor;

import java.util.concurrent.*;

/**
 * Coordinates the batch processing framework
 * Manages consumer thread pool and worker thread pool
 * @param <T> The type of DTO being processed
 */
public class BatchCoordinator<T> {
    
    private final BlockingQueue<BatchMessage<T>> queue;
    private final BatchAccumulator<T> accumulator;
    private final BatchProcessor<T> processor;
    private final CleanupStrategy cleanupStrategy;
    
    private final ExecutorService consumerExecutor;
    private final ExecutorService workerExecutor;
    private final BlockingQueue<String> batchIdQueue;
    
    private BatchConsumer<T> consumer;
    private volatile boolean started;

    private BatchCoordinator(Builder<T> builder) {
        this.queue = builder.queue;
        this.accumulator = new BatchAccumulator<>(builder.maxBatchSize);
        this.processor = builder.processor;
        this.cleanupStrategy = builder.cleanupStrategy;
        
        this.consumerExecutor = Executors.newFixedThreadPool(
            builder.consumerThreads,
            new NamedThreadFactory("BatchConsumer")
        );
        
        this.workerExecutor = Executors.newFixedThreadPool(
            builder.workerThreads,
            new NamedThreadFactory("BatchWorker")
        );
        
        this.batchIdQueue = new LinkedBlockingQueue<>();
        this.started = false;
    }

    /**
     * Starts the coordinator - begins consuming from queue
     */
    public void start() {
        if (started) {
            System.out.println("[BatchCoordinator] Already started");
            return;
        }

        System.out.println("[BatchCoordinator] Starting...");
        
        // Create consumer with callback to submit completed batch IDs
        consumer = new BatchConsumer<>(
            queue,
            accumulator,
            this::submitBatchIdForProcessing,
            1000L // 1 second poll timeout
        );
        
        // Submit consumer to executor
        consumerExecutor.submit(consumer);
        
        // Start worker dispatcher
        workerExecutor.submit(this::dispatchWorkers);
        
        started = true;
        System.out.println("[BatchCoordinator] Started successfully");
    }

    /**
     * Callback from consumer when a batch ID is complete
     */
    private void submitBatchIdForProcessing(String batchId) {
        try {
            System.out.println("[BatchCoordinator] Submitting batch ID for processing: " + batchId);
            batchIdQueue.put(batchId);
        } catch (InterruptedException e) {
            System.err.println("[BatchCoordinator] Interrupted while submitting batch ID: " + batchId);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Dispatcher that creates workers for completed batch IDs
     */
    private void dispatchWorkers() {
        System.out.println("[BatchCoordinator] Worker dispatcher started - thread: " + 
                          Thread.currentThread().getName());
        
        try {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    String batchId = batchIdQueue.poll(1, TimeUnit.SECONDS);
                    
                    if (batchId != null) {
                        System.out.println("[BatchCoordinator] Dispatching worker for batch ID: " + batchId);
                        
                        BatchWorker<T> worker = new BatchWorker<>(
                            batchId,
                            accumulator,
                            processor,
                            cleanupStrategy
                        );
                        
                        workerExecutor.submit(worker);
                    }
                    
                } catch (InterruptedException e) {
                    System.out.println("[BatchCoordinator] Worker dispatcher interrupted");
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } finally {
            System.out.println("[BatchCoordinator] Worker dispatcher stopped");
        }
    }

    /**
     * Initiates graceful shutdown
     * @param timeoutSeconds Maximum time to wait for shutdown
     * @return true if shutdown completed within timeout
     */
    public boolean shutdown(long timeoutSeconds) {
        System.out.println("[BatchCoordinator] Shutdown initiated, timeout: " + timeoutSeconds + "s");
        
        if (!started) {
            System.out.println("[BatchCoordinator] Not started, nothing to shutdown");
            return true;
        }

        // Signal consumer to stop
        if (consumer != null) {
            consumer.shutdown();
        }

        try {
            // Shutdown consumer executor
            System.out.println("[BatchCoordinator] Shutting down consumer executor...");
            consumerExecutor.shutdown();
            boolean consumerDone = consumerExecutor.awaitTermination(timeoutSeconds / 2, TimeUnit.SECONDS);
            
            if (!consumerDone) {
                System.out.println("[BatchCoordinator] Consumer executor did not terminate in time, forcing shutdown");
                consumerExecutor.shutdownNow();
            } else {
                System.out.println("[BatchCoordinator] Consumer executor shutdown complete");
            }

            // Shutdown worker executor
            System.out.println("[BatchCoordinator] Shutting down worker executor...");
            workerExecutor.shutdown();
            boolean workerDone = workerExecutor.awaitTermination(timeoutSeconds / 2, TimeUnit.SECONDS);
            
            if (!workerDone) {
                System.out.println("[BatchCoordinator] Worker executor did not terminate in time, forcing shutdown");
                workerExecutor.shutdownNow();
            } else {
                System.out.println("[BatchCoordinator] Worker executor shutdown complete");
            }

            boolean success = consumerDone && workerDone;
            System.out.println("[BatchCoordinator] Shutdown " + (success ? "successful" : "incomplete"));
            return success;
            
        } catch (InterruptedException e) {
            System.err.println("[BatchCoordinator] Shutdown interrupted");
            Thread.currentThread().interrupt();
            consumerExecutor.shutdownNow();
            workerExecutor.shutdownNow();
            return false;
        }
    }

    public boolean isStarted() {
        return started;
    }

    public int getQueueSize() {
        return queue.size();
    }

    public int getPendingBatchIds() {
        return batchIdQueue.size();
    }

    /**
     * Builder for BatchCoordinator
     */
    public static class Builder<T> {
        private BlockingQueue<BatchMessage<T>> queue;
        private int maxBatchSize = 5000;
        private BatchProcessor<T> processor;
        private CleanupStrategy cleanupStrategy = new NoOpCleanupStrategy();
        private int consumerThreads = 1;
        private int workerThreads = 4;

        public Builder<T> queue(BlockingQueue<BatchMessage<T>> queue) {
            this.queue = queue;
            return this;
        }

        public Builder<T> maxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Builder<T> processor(BatchProcessor<T> processor) {
            this.processor = processor;
            return this;
        }

        public Builder<T> cleanupStrategy(CleanupStrategy cleanupStrategy) {
            this.cleanupStrategy = cleanupStrategy;
            return this;
        }

        public Builder<T> consumerThreads(int consumerThreads) {
            this.consumerThreads = consumerThreads;
            return this;
        }

        public Builder<T> workerThreads(int workerThreads) {
            this.workerThreads = workerThreads;
            return this;
        }

        public BatchCoordinator<T> build() {
            if (queue == null) {
                throw new IllegalStateException("Queue must be set");
            }
            if (processor == null) {
                throw new IllegalStateException("Processor must be set");
            }
            return new BatchCoordinator<>(this);
        }
    }

    /**
     * Thread factory for named threads
     */
    private static class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private int counter = 0;

        public NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, prefix + "-" + (++counter));
        }
    }
}
