package com.chenjj.bigdata.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HdfsClientUtil {

    private static Logger logger = LoggerFactory.getLogger(HdfsClientUtil.class);

    private static Configuration conf = new Configuration();

    static {
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("yarn-site.xml");
    }

    /**
     * 上传
     * @param src
     * @param dst
     */
    public static void put(String src,String dst) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            fs.copyFromLocalFile(new Path(src),new Path(dst));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(fs != null){
                try {
                    fs.close();
                } catch (IOException e) {
                    logger.error("",e);
                }
            }
        }
    }


    /**
     * 下载
     * @param src
     * @param dst
     */
    public static void get(String src,String dst){
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            fs.copyToLocalFile(new Path(src),new Path(dst));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(fs != null){
                try {
                    fs.close();
                } catch (IOException e) {
                    logger.error("",e);
                }
            }
        }
    }

    /**
     * 删除文件/目录
     * @param path
     */
    public static void rm(String path){
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            fs.delete(new Path(path),true);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(fs != null){
                try {
                    fs.close();
                } catch (IOException e) {
                    logger.error("",e);
                }
            }
        }
    }

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","D:\\tools\\hadoop-2.7.3");
        String path = HdfsClientUtil.class.getClassLoader().getResource("").getPath();
        System.out.println("path = " + path);
        HdfsClientUtil.put(path+ "wordcountData/dream.txt","/test/mapreduce/input/");
    }
}
