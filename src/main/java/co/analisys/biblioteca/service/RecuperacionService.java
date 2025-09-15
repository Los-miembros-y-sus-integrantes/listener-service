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
    private static final List<String> TOPICS = Arrays.asList("ocupacion-clases", "resumen-entrenamiento");

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
                            guardarCheckpoint(record.topic(), record.partition(), record.offset(), "PROCESSED");
                            
                            // Commit manual después de procesar exitosamente
                            consumer.commitSync();
                            
                        } catch (Exception e) {
                            log.error("Error procesando record: topic={}, partition={}, offset={}, error={}",
                                    record.topic(), record.partition(), record.offset(), e.getMessage());
                            
                            guardarCheckpoint(record.topic(), record.partition(), record.offset(), "FAILED");
                            
                            // Estrategia de reintentos
                            intentarReintento(record, 3);
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
    private void guardarCheckpoint(String topic, int partition, long offset, String status) {
        try {
            Optional<KafkaCheckpoint> existingCheckpoint = checkpointRepository
                    .findByTopicNameAndPartitionNumberAndConsumerGroup(topic, partition, CONSUMER_GROUP);
            
            KafkaCheckpoint checkpoint;
            if (existingCheckpoint.isPresent()) {
                checkpoint = existingCheckpoint.get();
                checkpoint.setOffsetValue(offset);
                checkpoint.setStatus(status);
                checkpoint.setLastProcessed(LocalDateTime.now());
            } else {
                checkpoint = new KafkaCheckpoint(topic, partition, offset, CONSUMER_GROUP);
                checkpoint.setStatus(status);
            }
            
            checkpointRepository.save(checkpoint);
            
            log.debug("Checkpoint guardado: topic={}, partition={}, offset={}, status={}", 
                    topic, partition, offset, status);
            
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

    private void procesarResumenEntrenamiento(ConsumerRecord<String, String> record) {
        log.info("Procesando resumen de entrenamiento: {}", record.value());
        // Lógica específica para resúmenes de entrenamiento
    }

    private void intentarReintento(ConsumerRecord<String, String> record, int maxReintentos) {
        for (int intento = 1; intento <= maxReintentos; intento++) {
            try {
                log.info("Reintento {}/{} para record: topic={}, partition={}, offset={}", 
                        intento, maxReintentos, record.topic(), record.partition(), record.offset());
                
                Thread.sleep(1000 * intento); // Backoff exponencial
                
                procesarRecord(record);
                guardarCheckpoint(record.topic(), record.partition(), record.offset(), "PROCESSED");
                
                log.info("Reintento exitoso en intento {}", intento);
                return;
                
            } catch (Exception e) {
                log.error("Reintento {} falló: {}", intento, e.getMessage());
                guardarCheckpoint(record.topic(), record.partition(), record.offset(), "RETRYING");
            }
        }
        
        log.error("Todos los reintentos fallaron para record: topic={}, partition={}, offset={}", 
                record.topic(), record.partition(), record.offset());
        guardarCheckpoint(record.topic(), record.partition(), record.offset(), "FAILED");
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