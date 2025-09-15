package co.analisys.biblioteca.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import co.analisys.biblioteca.dto.OcupacionClaseDTO;
import com.google.gson.Gson;


@Service
public class OcupacionClaseConsumer {
    @KafkaListener(topics = "ocupacion-clases", groupId = "monitoreo-grupo")
    public void consumirActualizacionOcupacion(String ocupacion) {
        System.out.println("Mensaje recibido: " + ocupacion);
    }
}