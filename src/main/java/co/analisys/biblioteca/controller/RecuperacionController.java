package co.analisys.biblioteca.controller;

import co.analisys.biblioteca.model.KafkaCheckpoint;
import co.analisys.biblioteca.service.RecuperacionService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/recuperacion")
@RequiredArgsConstructor
public class RecuperacionController {

    private final RecuperacionService recuperacionService;

    @PostMapping("/iniciar")
    public ResponseEntity<String> iniciarProcesamiento() {
        try {
            recuperacionService.iniciarProcesamiento();
            return ResponseEntity.ok("Procesamiento con recuperación iniciado correctamente");
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("Error iniciando procesamiento: " + e.getMessage());
        }
    }

    @PostMapping("/detener")
    public ResponseEntity<String> detenerProcesamiento() {
        try {
            recuperacionService.detenerProcesamiento();
            return ResponseEntity.ok("Procesamiento detenido correctamente");
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("Error deteniendo procesamiento: " + e.getMessage());
        }
    }

    @GetMapping("/checkpoints")
    public ResponseEntity<List<KafkaCheckpoint>> obtenerCheckpoints() {
        try {
            List<KafkaCheckpoint> checkpoints = recuperacionService.obtenerEstadoCheckpoints();
            return ResponseEntity.ok(checkpoints);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(null);
        }
    }

    @DeleteMapping("/checkpoints/{topic}")
    public ResponseEntity<String> limpiarCheckpoints(@PathVariable String topic) {
        try {
            recuperacionService.limpiarCheckpoints(topic);
            return ResponseEntity.ok("Checkpoints limpiados para topic: " + topic);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("Error limpiando checkpoints: " + e.getMessage());
        }
    }

    @GetMapping("/status")
    public ResponseEntity<String> obtenerEstado() {
        return ResponseEntity.ok("""
                Sistema de Recuperación de Fallos - Estado
                
                Endpoints disponibles:
                • POST /recuperacion/iniciar - Iniciar procesamiento
                • POST /recuperacion/detener - Detener procesamiento  
                • GET /recuperacion/checkpoints - Ver checkpoints
                • DELETE /recuperacion/checkpoints/{topic} - Limpiar checkpoints
                
                Topics monitoreados: ocupacion-clases, resumen-entrenamiento
                Checkpoints almacenados en base de datos H2
                Reintentos automáticos: 3 intentos con backoff exponencial
                """);
    }
}