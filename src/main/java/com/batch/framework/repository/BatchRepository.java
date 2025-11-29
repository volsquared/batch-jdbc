package com.batch.framework.repository;

import com.batch.framework.model.Batch;
import com.batch.framework.processor.BatchProcessingException;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityTransaction;

import java.util.List;

/**
 * Abstract repository using Jakarta Persistence for batch operations
 * Subclass this and implement persist() for your specific entity type
 * @param <T> The DTO type
 * @param <E> The Entity type (JPA entity)
 */
public abstract class BatchRepository<T, E> {
    
    protected final EntityManager entityManager;

    public BatchRepository(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    /**
     * Persists a batch of items within a transaction
     * @param batch The batch to persist
     * @throws BatchProcessingException if persistence fails
     */
    public void saveBatch(Batch<T> batch) throws BatchProcessingException {
        EntityTransaction transaction = null;
        
        try {
            transaction = entityManager.getTransaction();
            transaction.begin();
            
            System.out.println("[BatchRepository] Starting transaction for batch: " + 
                              batch.getBatchId() + ", sequence: " + batch.getSequenceNumber());
            
            List<E> entities = convertToEntities(batch.getItems());
            
            for (E entity : entities) {
                entityManager.persist(entity);
            }
            
            entityManager.flush();
            transaction.commit();
            
            System.out.println("[BatchRepository] Successfully committed batch: " + 
                              batch.getBatchId() + ", sequence: " + batch.getSequenceNumber() +
                              ", items: " + batch.size());
            
        } catch (Exception e) {
            System.err.println("[BatchRepository] Error processing batch: " + 
                              batch.getBatchId() + ", sequence: " + batch.getSequenceNumber());
            e.printStackTrace();
            
            if (transaction != null && transaction.isActive()) {
                try {
                    transaction.rollback();
                    System.out.println("[BatchRepository] Transaction rolled back for batch: " + 
                                      batch.getBatchId());
                } catch (Exception rollbackEx) {
                    System.err.println("[BatchRepository] Rollback failed: " + rollbackEx.getMessage());
                    rollbackEx.printStackTrace();
                }
            }
            
            throw new BatchProcessingException(
                batch.getBatchId(), 
                batch.getSequenceNumber(),
                "Failed to persist batch", 
                e
            );
        }
    }

    /**
     * Converts DTOs to JPA entities
     * Implement this in your concrete repository
     * @param dtos The DTOs to convert
     * @return List of JPA entities
     */
    protected abstract List<E> convertToEntities(List<T> dtos);

    /**
     * Optional: cleanup partial writes for a batch ID
     * Override this to implement batch-specific cleanup logic
     * @param batchId The batch ID to clean up
     */
    public void cleanupBatch(String batchId) {
        System.out.println("[BatchRepository] Cleanup requested for batch: " + batchId + 
                          " (override cleanupBatch() to implement)");
        
        // Example pseudocode:
        // EntityTransaction tx = entityManager.getTransaction();
        // try {
        //     tx.begin();
        //     entityManager.createQuery("DELETE FROM YourEntity e WHERE e.batchId = :batchId")
        //                  .setParameter("batchId", batchId)
        //                  .executeUpdate();
        //     tx.commit();
        // } catch (Exception e) {
        //     if (tx.isActive()) tx.rollback();
        //     throw e;
        // }
    }
}
