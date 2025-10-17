package org.example;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.StringTokenizer;

class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text word = new Text();
        final IntWritable numberOne = new IntWritable(1);

        public void map(LongWritable key, Text inputValue, Context context) throws IOException, InterruptedException {
            String line = inputValue.toString();
            StringTokenizer sTokenizer= new StringTokenizer(line);

            while(sTokenizer.hasMoreTokens()){
                word.set(sTokenizer.nextToken());
                context.write(word, numberOne);
            }

        }

    }
