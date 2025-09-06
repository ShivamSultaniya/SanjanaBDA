package org.example.touristflow;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer that sums passenger counts per route key ("origin->destination").
 */
public class FlowReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable sumWritable = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        // Cap at Integer.MAX_VALUE to stay within IntWritable; unlikely to overflow for demo-sized data
        if (sum > Integer.MAX_VALUE) {
            sum = Integer.MAX_VALUE;
        }
        sumWritable.set((int) sum);
        context.write(key, sumWritable);
    }
}


