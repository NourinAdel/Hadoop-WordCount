package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        if (args.length != 2) {
            System.err.println("Usage: WordCount <input> <output>");
            System.exit(1);
        }

        System.out.println("DEBUG: args length = " + args.length);
        for (int i = 0; i < args.length; i++) {
            System.out.println("DEBUG: args[" + i + "] = " + args[i]);
        }


        //Configure job using default settings
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "word count");

        //Looks in the same JAR file that contains WordCount.class to find all other classes
        // and sets mapper and reducer classes
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);

        //Tells hadoop that key, value pairs output from reduces should be
        //in the form of Text,IntWritable
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //Set arguments for hadoop from the running command/HDFS
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //Run the job and exit with success(0) if successful or (1) if not
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}