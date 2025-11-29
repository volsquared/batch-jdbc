package com.batch.framework.repository;

import com.batch.framework.dto.PersonDTO;
import jakarta.persistence.EntityManager;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Example repository implementation for PersonDTO
 * Replace PersonEntity with your actual JPA entity
 */
public class PersonRepository extends BatchRepository<PersonDTO, PersonEntity> {

    public PersonRepository(EntityManager entityManager) {
        super(entityManager);
    }

    @Override
    protected List<PersonEntity> convertToEntities(List<PersonDTO> dtos) {
        System.out.println("[PersonRepository] Converting " + dtos.size() + " DTOs to entities");
        
        return dtos.stream()
                   .map(this::toEntity)
                   .collect(Collectors.toList());
    }

    private PersonEntity toEntity(PersonDTO dto) {
        // Pseudocode - replace with your actual entity mapping
        PersonEntity entity = new PersonEntity();
        // entity.setId(dto.getId());
        // entity.setFirstName(dto.getFirstName());
        // entity.setLastName(dto.getLastName());
        // entity.setEmail(dto.getEmail());
        return entity;
    }

    @Override
    public void cleanupBatch(String batchId) {
        System.out.println("[PersonRepository] Cleaning up batch: " + batchId);
        
        // Pseudocode - implement your cleanup logic
        // EntityTransaction tx = entityManager.getTransaction();
        // try {
        //     tx.begin();
        //     int deleted = entityManager
        //         .createQuery("DELETE FROM PersonEntity p WHERE p.batchId = :batchId")
        //         .setParameter("batchId", batchId)
        //         .executeUpdate();
        //     tx.commit();
        //     System.out.println("[PersonRepository] Deleted " + deleted + " entities for batch: " + batchId);
        // } catch (Exception e) {
        //     if (tx.isActive()) tx.rollback();
        //     throw new RuntimeException("Cleanup failed for batch: " + batchId, e);
        // }
    }
}

/**
 * Placeholder JPA Entity - replace with your actual entity
 */
class PersonEntity {
    // @Id
    // @GeneratedValue
    // private Long id;
    // 
    // private String firstName;
    // private String lastName;
    // private String email;
    // private String batchId;
    // 
    // // getters/setters...
}
