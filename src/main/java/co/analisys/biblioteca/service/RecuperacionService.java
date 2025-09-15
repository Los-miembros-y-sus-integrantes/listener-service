package co.analisys.biblioteca.service;

import co.analisys.biblioteca.model.KafkaCheckpoint;
import co.analisys.biblioteca.repository.KafkaCheckpointRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
@RequiredArgsConstructor
@Slf4j
public class RecuperacionService {

    private final KafkaCheckpointRepository checkpointRepository;
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    private KafkaConsumer<String, String> consumer;
    private ExecutorService executorService;
    private volatile boolean running = false;
    
    private static final String CONSUMER_GROUP = "recuperacion-grupo";
    private static final List<String> TOPICS = Arrays.asList("ocupacion-clases", "datos-entrenamiento", "resumen-entrenamiento");

    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Manual commit for checkpoint control
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        consumer = new KafkaConsumer<>(props);
        executorService = Executors.newSingleThreadExecutor();
        
        log.info("RecuperacionService inicializado correctamente");
    }

    public void iniciarProcesamiento() {
        if (running) {
            log.warn("El procesamiento ya está en ejecución");
            return;
        }
        
        running = true;
        executorService.submit(() -> {
            try {
                log.info("Iniciando procesamiento con recuperación de fallos...");
                
                // Suscribirse a los topics
                consumer.subscribe(TOPICS);
                
                // Cargar último offset procesado desde la base de datos
                Map<TopicPartition, Long> ultimoOffsetProcesado = cargarUltimoOffset();
                
                // Posicionar el consumer en los últimos offsets procesados
                if (!ultimoOffsetProcesado.isEmpty()) {
                    log.info("Restaurando posición desde checkpoints: {}", ultimoOffsetProcesado);
                    for (Map.Entry<TopicPartition, Long> entry : ultimoOffsetProcesado.entrySet()) {
                        consumer.seek(entry.getKey(), entry.getValue() + 1); // +1 para procesar desde el siguiente
                    }
                }
                
                while (running) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            procesarRecord(record);
                            guardarCheckpoint(record, "PROCESSED", null);
                            
                            // Commit manual después de procesar exitosamente
                            consumer.commitSync();
                            
                        } catch (Exception e) {
                            log.error("Error procesando record: topic={}, partition={}, offset={}, error={}",
                                    record.topic(), record.partition(), record.offset(), e.getMessage());
                            
                            guardarCheckpoint(record, "FAILED", e.getMessage());
                            
                            // Estrategia de reintentos
                            intentarReintento(record);
                        }
                    }
                }
                
            } catch (Exception e) {
                log.error("Error en el procesamiento principal: {}", e.getMessage(), e);
            } finally {
                consumer.close();
                log.info("Consumer cerrado correctamente");
            }
        });
        
        log.info("Procesamiento iniciado en background");
    }

    private Map<TopicPartition, Long> cargarUltimoOffset() {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        
        try {
            List<KafkaCheckpoint> checkpoints = checkpointRepository.findLatestByConsumerGroup(CONSUMER_GROUP);
            
            for (KafkaCheckpoint checkpoint : checkpoints) {
                if ("PROCESSED".equals(checkpoint.getStatus())) {
                    TopicPartition tp = new TopicPartition(checkpoint.getTopicName(), checkpoint.getPartitionNumber());
                    offsets.put(tp, checkpoint.getOffsetValue());
                }
            }
            
            log.info("Cargados {} checkpoints desde la base de datos", offsets.size());
            
        } catch (Exception e) {
            log.error("Error cargando checkpoints: {}", e.getMessage());
        }
        
        return offsets;
    }

    @Transactional
    private void guardarCheckpoint(ConsumerRecord<String, String> record, String status, String errorMessage) {
        try {
            Optional<KafkaCheckpoint> existingCheckpoint = checkpointRepository
                    .findByTopicNameAndPartitionNumberAndConsumerGroup(record.topic(), record.partition(), CONSUMER_GROUP);
            
            KafkaCheckpoint checkpoint;
            if (existingCheckpoint.isPresent()) {
                checkpoint = existingCheckpoint.get();
                checkpoint.setOffsetValue(record.offset());
                checkpoint.setStatus(status);
                checkpoint.setMessageKey(record.key());
                checkpoint.setMessagePayload(record.value());
                checkpoint.setErrorMessage(errorMessage);
                checkpoint.setLastProcessed(LocalDateTime.now());
            } else {
                checkpoint = new KafkaCheckpoint(record.topic(), record.partition(), record.offset(), 
                                               CONSUMER_GROUP, record.key(), record.value(), status);
                checkpoint.setErrorMessage(errorMessage);
            }
            
            checkpointRepository.save(checkpoint);
            
            log.debug("Checkpoint guardado: topic={}, partition={}, offset={}, status={}, payload_length={}", 
                    record.topic(), record.partition(), record.offset(), status, 
                    record.value() != null ? record.value().length() : 0);
            
        } catch (Exception e) {
            log.error("Error guardando checkpoint: {}", e.getMessage());
        }
    }

    private void procesarRecord(ConsumerRecord<String, String> record) {
        log.info("Procesando mensaje: topic={}, partition={}, offset={}, key={}, value={}", 
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
        
        // Aquí iría la lógica específica de procesamiento según el topic
        switch (record.topic()) {
            case "ocupacion-clases":
                procesarOcupacionClase(record);
                break;
            case "datos-entrenamiento":
                procesarDatosEntrenamiento(record);
                break;
            case "resumen-entrenamiento":
                procesarResumenEntrenamiento(record);
                break;
            default:
                log.warn("Topic no reconocido: {}", record.topic());
        }
        
        // Simular un pequeño delay de procesamiento
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void procesarOcupacionClase(ConsumerRecord<String, String> record) {
        log.info("Procesando ocupación de clase: {}", record.value());
        // Lógica específica para ocupación de clases
    }

    private void procesarDatosEntrenamiento(ConsumerRecord<String, String> record) {
        log.info("Procesando datos de entrenamiento: {}", record.value());
        // Lógica específica para datos de entrenamiento sin procesar
    }

    private void procesarResumenEntrenamiento(ConsumerRecord<String, String> record) {
        log.info("Procesando resumen de entrenamiento: {}", record.value());
        // Lógica específica para resúmenes de entrenamiento agregados
    }

    private void intentarReintento(ConsumerRecord<String, String> record) {
        try {
            // Reintentar procesar el mensaje según su topic
            switch (record.topic()) {
                case "ocupacion-clases":
                    procesarOcupacionClase(record);
                    break;
                case "datos-entrenamiento":
                    procesarDatosEntrenamiento(record);
                    break;
                case "resumen-entrenamiento":
                    procesarResumenEntrenamiento(record);
                    break;
                default:
                    throw new RuntimeException("Topic no reconocido: " + record.topic());
            }
            
            log.info("Mensaje reprocesado exitosamente: topic={}, partition={}, offset={}", 
                    record.topic(), record.partition(), record.offset());
            
            // Marcar como procesado exitosamente
            guardarCheckpoint(record, "PROCESSED", null);
            
        } catch (Exception e) {
            String errorMessage = "Error en reintento " + e.getMessage();
            log.error("Error reintentando procesar mensaje: topic={}, partition={}, offset={}, error={}", 
                    record.topic(), record.partition(), record.offset(), e.getMessage());
            
            // Marcar como reintentando para el siguiente intento
            guardarCheckpoint(record, "RETRYING", errorMessage);
        }
        
        // Si todos los reintentos fallan, marcar como fallido permanentemente
        guardarCheckpoint(record, "FAILED", "Excedido numero maximo de reintentos");
    }

    public void detenerProcesamiento() {
        log.info("Deteniendo procesamiento...");
        running = false;
    }

    public List<KafkaCheckpoint> obtenerEstadoCheckpoints() {
        return checkpointRepository.findLatestByConsumerGroup(CONSUMER_GROUP);
    }

    public void limpiarCheckpoints(String topic) {
        log.info("Limpiando checkpoints para topic: {}", topic);
        checkpointRepository.deleteByTopicNameAndConsumerGroup(topic, CONSUMER_GROUP);
    }

    @PreDestroy
    public void cleanup() {
        detenerProcesamiento();
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
        }
    }
}