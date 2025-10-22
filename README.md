# Hadoop Program setup

Three main classes are usually needed to perform a HDFS job: **Main**, **Mapper**, and **Reducer**.

For the example below, it's a simple Word Counter program.

## 1. The Main Class (`WordCount.java`)

This has the project setup, starting with those 2 lines:

```java
Configuration config = new Configuration();
Job job = Job.getInstance(config, "word count");
```

Then, the job is configured by specifying the main class (Jar class), **mapper** class, and **reducer** class. Also, it specifies how the output from the reducer looks like.

```java
// Looks in the same JAR file that contains WordCount.class to find all other classes
// and sets mapper and reducer classes
job.setJarByClass(WordCount.class);
job.setMapperClass(WordMapper.class);
job.setReducerClass(WordReducer.class);

// Tells hadoop that key, value pairs output from reduces should be
// in the form of Text,IntWritable
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
```

It should also specify the input and the output for the program. This sets the arguments for hadoop from the running command/HDFS

```java
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
```

Finally, the job is started. This runs the job and exits with (0) if successful or (1) if not

```java
System.exit(job.waitForCompletion(true) ? 0 : 1);
```

## 2. The Mapper (`WordMapper.java`)

This class does the mapping work. It's what runs in parallel on many different machines, each processing a different chunk of the input.

This class has to extend the Mapper class. It has to define the input and output types:

**Input:** `<LongWritable, Text>` representing the byte offset of the line in the file. It's usually ignored. The text represents the input text to be processed. 

**Output:** `<Text, IntWritable>` representing the output key and value (e.g. `(hello, 2)`)

```java
class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    ...
}
```

A reusable object called IntWritable is used to be the value in the output map. It's always 1 because each text word will be counted once. It's not the mapper's job to sum them up.

```java
Text word = new Text();
final IntWritable numberOne = new IntWritable(1);
```

The main logic of the mapper is in the map function:
```java

public void map(LongWritable key, Text inputValue, Context context) throws IOException, InterruptedException {
    String line = inputValue.toString();
    StringTokenizer sTokenizer= new StringTokenizer(line);

    while(sTokenizer.hasMoreTokens()){
        word.set(sTokenizer.nextToken());
        context.write(word, numberOne);
    }
}
```

This function tokenizes the string and keeps outputting each word in a map with key and values `(<word>, 1)`.
If the input text is "hi hello hi hey", the output will be:
- `(hi, 1)`
- `(hello, 1)`
- `(hi, 1)`
- `(hey, 1)`

## 3. The Reducer (`WordReducer.java`)

After the mappers finish, Hadoop shuffles the data and groups all the outputs by key. The reducer's job is to process each group.

This class extends the Reducer class, and it also has to also define the input and output types:

**Input:** `<Text, IntWritable>` representing the word and all the counts from the mappers. It should be something like `(hi, [1, 1, 1])`
**Output:** `<Text, IntWritable>` representing the word and the total count. It should be something like `(hi, 3)`

```java
public class WordReducer extends Reducer <Text, IntWritable, Text, IntWritable> {
    ...
}
```

The main logic of the reducer is in the reduce function:

```java
public void reduce(Text key, Iterable<IntWritable> counts , Context context) throws IOException, InterruptedException {
    int sum = 0;

    for (IntWritable count : counts){
        sum += count.get();
    }

    sumWritable.set(sum);
    context.write(key, sumWritable);
}
```

This function iterates through all the counts for a given word, sums them up, and outputs the total count for that word.