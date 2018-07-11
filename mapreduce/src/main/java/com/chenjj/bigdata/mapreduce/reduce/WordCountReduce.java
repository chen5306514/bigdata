package com.chenjj.bigdata.mapreduce.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        logger.info("wordcount reduce run,key:{},value:{}",key,values);
        int sum = 0;
        IntWritable val;
        for (Iterator i = values.iterator(); i.hasNext(); sum += val.get()) {
            val = (IntWritable) i.next();
        }
        this.result.set(sum);
        context.write(key, this.result);

    }
}
