package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordReducer extends Reducer <Text, IntWritable, Text, IntWritable> {

    IntWritable sumWritable = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> counts , Context context) throws IOException, InterruptedException {
    int sum = 0;

    for (IntWritable count : counts){
        sum += count.get();
    }

    sumWritable.set(sum);
    context.write(key, sumWritable);
    }

}
