package org.example.touristflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper that parses lines of CSV with columns: origin_country,destination_country,passengers
 * Emits key as "origin->destination" and value as passengers (IntWritable).
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE_INT = new IntWritable(1); // placeholder to avoid re-allocation if needed
    private final Text routeKey = new Text();
    private final IntWritable passengerCountWritable = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;
        }

        // Skip header if present
        String lower = line.toLowerCase();
        if (lower.startsWith("origin") && lower.contains("destination") && lower.contains("passenger")) {
            return;
        }

        List<String> fields = splitCsv(line);
        if (fields.size() < 3) {
            return; // malformed
        }

        String origin = fields.get(0).trim();
        String destination = fields.get(1).trim();
        String passengersStr = fields.get(2).trim();

        if (origin.isEmpty() || destination.isEmpty()) {
            return;
        }

        int passengers;
        try {
            passengers = Integer.parseInt(passengersStr);
        } catch (NumberFormatException ex) {
            return; // malformed numeric field
        }

        if (passengers <= 0) {
            return; // ignore non-positive counts
        }

        routeKey.set(origin + "->" + destination);
        passengerCountWritable.set(passengers);
        context.write(routeKey, passengerCountWritable);
    }

    /**
     * Very small CSV splitter that supports quoted values containing commas.
     */
    private static List<String> splitCsv(String line) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    // Escaped quote
                    current.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                tokens.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        tokens.add(current.toString());
        return tokens;
    }
}


