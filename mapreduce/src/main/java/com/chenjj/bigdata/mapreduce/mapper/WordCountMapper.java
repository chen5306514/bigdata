package com.chenjj.bigdata.mapreduce.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>{
    private Logger logger = LoggerFactory.getLogger(getClass());
    public static final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        logger.info("wordcount mapper run,key:{},value:{}",key,value);
        //将一行数据转换成一个一个单词，分隔符为：空格\t\n\r\f
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            this.word.set(itr.nextToken());
            context.write(this.word, one);//输出给reduce,reduce接收的key=this.word,value=one
        }
    }

}
