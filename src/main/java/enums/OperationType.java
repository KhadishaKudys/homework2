package enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;

@Getter
@AllArgsConstructor
public enum OperationType {
    READ_MULTIPLE_TUPLES(0.0, 0.6),
    INSERT_ENTRY(0.6, 0.7),
    READ_WHOLE_TABLE(0.7, 0.8),
    UPDATE_IN_RANGES(0.8, Double.POSITIVE_INFINITY);

    private final double min;
    private final double max;

    public static OperationType fromProbability(final double p) {
        return Arrays.stream(values())
                .filter(operation -> p >= operation.min && p < operation.max)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Invalid probability: " + p));
    }
}




