package com.chenjj.bigdata.zk.client.test;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class ZkClientTest {

    static Logger logger = LoggerFactory.getLogger(ZkClientTest.class);

    /** zookeeper地址 */
    static final String ZK_ADDR = "127.0.0.1:2181";
    /** session超时时间 */
    static final int SESSION_OUTTIME = 1000;//ms


    public static void main(String[] args) throws InterruptedException {
        ZkClient zkClient = new ZkClient(new ZkConnection(ZK_ADDR),SESSION_OUTTIME);

        String path = "/hds/microservice/CoreSystemSocketServiceKey";

        if(!zkClient.exists(path)){
            zkClient.createPersistent(path, true);
        }

        Object data = zkClient.readData(path);

        if (null == data){
            logger.info("data is null ");
            zkClient.writeData(path,UUID.randomUUID().toString().replace("-","").substring(0,8));
            data = zkClient.readData(path);
            logger.info("data:{}",data);
        }else {
            logger.info("data:{}",data);
        }

        zkClient.subscribeDataChanges(path, new IZkDataListener() {
            public void handleDataChange(String dataPath, Object data) throws Exception {
                logger.info("data of path {} hasChanged, new data:{}",dataPath,data);
            }

            public void handleDataDeleted(String dataPath) throws Exception {

            }
        });

        Thread.sleep(600*1000);
        zkClient.writeData(path,UUID.randomUUID().toString().replace("-","").substring(0,8));

        data = zkClient.readData(path);
        logger.info("data:{}",data);
    }
}
