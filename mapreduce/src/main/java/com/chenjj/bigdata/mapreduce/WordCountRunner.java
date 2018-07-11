package com.chenjj.bigdata.mapreduce;

import com.chenjj.bigdata.hdfs.client.HdfsClientUtil;
import com.chenjj.bigdata.mapreduce.mapper.WordCountMapper;
import com.chenjj.bigdata.mapreduce.reduce.WordCountReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class WordCountRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //hadoop home
        System.setProperty("hadoop.home.dir","D:\\tools\\hadoop-2.7.3");
        System.setProperty("HADOOP_USER_NAME","root");//使用root用户操作hdfs
        System.load("D:\\HdfsClient\\conf\\bin\\hadoop64.dll");

        //hadoop配置信息
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("yarn-site.xml");
        conf.set("mapred.job.tracker","local"); //本地运行，可以用于debug

        //定义变量
        String classPath = WordCountRunner.class.getClassLoader().getResource("").getPath();
        String hdfsInputPath = "/test/mapreduce/input/";
        String hdfsOutPath = "/test/mapreduce/output/";
        String [] localDataPath = {classPath+"wordcountData/dream.txt",
                                   classPath+"wordcountData/dream1.txt"};

        //上传本地文件到Hdfs
        for (int i = 0; i < localDataPath.length ; i++) {
            HdfsClientUtil.put(localDataPath[i], hdfsInputPath+"file"+i);
        }

        //删除输出目录
        HdfsClientUtil.rm(hdfsOutPath);

        //定义job
        Job job = Job.getInstance(conf, "WordCount");
        job.setJarByClass(WordCountRunner.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(hdfsInputPath));//mapper的输入文件,如果是个目录，则目标是该目录下的所有文件。
        FileOutputFormat.setOutputPath(job, new Path(hdfsOutPath));//reduce的输出目录（如果没有reduce，则是mapper的输出目录）

        //启动job并等待job完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
