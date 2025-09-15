package co.analisys.biblioteca.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ResumenEntrenamiento {
    private int totalDuration;
    private int sessionCount;
}