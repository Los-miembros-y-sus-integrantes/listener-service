package co.analisys.biblioteca.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "kafka_checkpoint")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class KafkaCheckpoint {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false)
    private String topicName;
    
    @Column(nullable = false)
    private Integer partitionNumber;
    
    @Column(nullable = false)
    private Long offsetValue;
    
    @Column(nullable = false)
    private String consumerGroup;
    
    @Column(nullable = false)
    private LocalDateTime lastProcessed;
    
    @Column
    private String status; // "PROCESSED", "FAILED", "RETRYING"
    
    public KafkaCheckpoint(String topicName, Integer partitionNumber, Long offsetValue, String consumerGroup) {
        this.topicName = topicName;
        this.partitionNumber = partitionNumber;
        this.offsetValue = offsetValue;
        this.consumerGroup = consumerGroup;
        this.lastProcessed = LocalDateTime.now();
        this.status = "PROCESSED";
    }
}