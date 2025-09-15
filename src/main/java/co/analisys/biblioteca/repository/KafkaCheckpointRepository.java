package co.analisys.biblioteca.repository;

import co.analisys.biblioteca.model.KafkaCheckpoint;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface KafkaCheckpointRepository extends JpaRepository<KafkaCheckpoint, Long> {
    
    Optional<KafkaCheckpoint> findByTopicNameAndPartitionNumberAndConsumerGroup(
            String topicName, Integer partitionNumber, String consumerGroup);
    
    List<KafkaCheckpoint> findByConsumerGroupAndStatus(String consumerGroup, String status);
    
    @Query("SELECT c FROM KafkaCheckpoint c WHERE c.consumerGroup = :consumerGroup ORDER BY c.lastProcessed DESC")
    List<KafkaCheckpoint> findLatestByConsumerGroup(@Param("consumerGroup") String consumerGroup);
    
    void deleteByTopicNameAndConsumerGroup(String topicName, String consumerGroup);
}