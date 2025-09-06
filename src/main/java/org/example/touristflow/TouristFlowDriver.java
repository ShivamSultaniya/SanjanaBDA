package org.example.touristflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver program to compute total tourist flows between countries.
 *
 * Input CSV columns: origin_country,destination_country,passengers
 * Output: key=origin->destination, value=sum(passengers)
 */
public class TouristFlowDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: TouristFlowDriver <input-path> <output-path>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        Configuration conf = new Configuration();
        // Ensure local execution when launched via `mvn exec:java`
        conf.setIfUnset("mapreduce.framework.name", "local");
        conf.setIfUnset("fs.defaultFS", "file:///");

        Job job = Job.getInstance(conf, "Tourist Flow Between Countries");
        job.setJarByClass(TouristFlowDriver.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 2);
    }
}


