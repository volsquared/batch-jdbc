package com.batch.framework.processor;

/**
 * Exception thrown when batch processing fails
 */
public class BatchProcessingException extends Exception {
    
    private final String batchId;
    private final int sequenceNumber;

    public BatchProcessingException(String batchId, int sequenceNumber, String message) {
        super(message);
        this.batchId = batchId;
        this.sequenceNumber = sequenceNumber;
    }

    public BatchProcessingException(String batchId, int sequenceNumber, String message, Throwable cause) {
        super(message, cause);
        this.batchId = batchId;
        this.sequenceNumber = sequenceNumber;
    }

    public String getBatchId() {
        return batchId;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public String toString() {
        return "BatchProcessingException{" +
               "batchId='" + batchId + '\'' +
               ", sequenceNumber=" + sequenceNumber +
               ", message='" + getMessage() + '\'' +
               '}';
    }
}
