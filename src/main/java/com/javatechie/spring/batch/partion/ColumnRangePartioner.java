package com.javatechie.spring.batch.partion;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import java.util.HashMap;
import java.util.Map;

public class ColumnRangePartioner implements Partitioner {

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        int min = 1;
        int max = 1000;

        int range = max - min + 1; // Assure qu'on prend bien tout l'intervalle
        if (gridSize > range) {
            gridSize = range; // Évite des partitions vides
        }

        int targetSize = (int) Math.ceil((double) range / gridSize);
        System.out.println("targetSize = " + targetSize);

        Map<String, ExecutionContext> result = new HashMap<>();

        int number = 0;
        int start = min;
        int end = start + targetSize - 1;

        while (start <= max) {
            ExecutionContext value = new ExecutionContext();
            result.put("partition" + number, value);

            // Assurer que `end` ne dépasse pas `max`
            end = Math.min(end, max);

            value.put("minValue", start);
            value.put("maxValue", end);

            System.out.println("Partition " + number + " -> Min: " + start + ", Max: " + end);

            start += targetSize;
            end += targetSize;
            number++;
        }

        System.out.println("Final partition result: " + result);
        return result;
    }
}
