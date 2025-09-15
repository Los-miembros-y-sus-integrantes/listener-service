package co.analisys.biblioteca.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;

@RestController
@RequestMapping("/listener")
public class ListenerController {

    @GetMapping("/status")
    public String getStatus() {
        return "Listener Service está activo y escuchando el topic 'resumen-entrenamiento'. Timestamp: " + LocalDateTime.now();
    }

    @GetMapping("/info")
    public String getInfo() {
        return """
                Listener Service - Información
                
                Topic escuchado: resumen-entrenamiento
                Group ID: listener-service-group
                Puerto: 8085
                
                Este servicio escucha automáticamente los resúmenes de entrenamiento
                publicados por el trainer-service y los imprime en la consola.
                """;
    }
}