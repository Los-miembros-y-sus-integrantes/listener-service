package co.analisys.biblioteca.service;

import co.analisys.biblioteca.model.ResumenEntrenamiento;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Service
@Slf4j
public class ResumenEntrenamientoListener {

    @KafkaListener(
            topics = "resumen-entrenamiento",
            containerFactory = "resumenKafkaListenerContainerFactory"
    )
    public void escucharResumenEntrenamiento(
            @Payload ResumenEntrenamiento resumen,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        
        System.out.println("\n" + "=".repeat(80));
        System.out.println("NUEVO RESUMEN DE ENTRENAMIENTO RECIBIDO");
        System.out.println("=".repeat(80));
        System.out.println("Timestamp: " + timestamp);
        System.out.println("Key: " + key);
        System.out.println("Topic: " + topic);
        System.out.println("Partition: " + partition);
        System.out.println("Offset: " + offset);
        System.out.println("-".repeat(40));
        System.out.println("RESUMEN DE DATOS:");
        System.out.println("Duración Total: " + resumen.getTotalDuration() + " minutos");
        System.out.println("Número de Sesiones: " + resumen.getSessionCount());
        
        if (resumen.getSessionCount() > 0) {
            double promedioDuracion = (double) resumen.getTotalDuration() / resumen.getSessionCount();
            System.out.println("Duración Promedio por Sesión: " + String.format("%.2f", promedioDuracion) + " minutos");
        }
        
        System.out.println("=".repeat(80));
        System.out.println();
        
        // Log también para el sistema de logging
        log.info("Resumen de entrenamiento procesado - Key: {}, Duración Total: {} min, Sesiones: {}", 
                key, resumen.getTotalDuration(), resumen.getSessionCount());
    }
}