package co.analisys.biblioteca.dto;

import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class OcupacionClaseDTO {
    
    public Long idClase;
    public Integer ocupacion;
    public LocalDateTime hora;
}
