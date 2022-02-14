package com.lagou.zk.demo;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.io.IOException;


public class PublishTest {

    static ZkMain zk = new ZkMain();
    static ZkClient zkClient = null;

    @Test
    public void test() throws InterruptedException, IOException {
        String zkPath = "/dbconfig/mysql";
        String dbconfig = "jdbc:mysql://linux123:3306/test?useSSL=false\t" +
                "root\t" +
                "12345678";
        //              获取zk连接
        zkClient = zk.connectZK();
        //        使用自定义序列化
        zkClient.setZkSerializer(new ZkStrSerializer());
        //        创建zk节点并将常量文件中的数据库配置信息写入zk
        zk.registerServerInfo(dbconfig);

        //        使用配置信息初始化数据库连接信息
        DataSource dataSource = DruidMain.getConf(zkClient, zkPath);
        //        执行查询
        DruidMain.execQuery(dataSource);
        Thread.sleep(3000);

        //        匿名内部类监听节点数据是否变化
        zkClient.subscribeDataChanges(zkPath, new IZkDataListener() {
            @Override
            public void handleDataChange(String s, Object o) throws Exception {

          // 配置信息变化，重新获取配置信息并执行查询
                System.out.println("数据库配置信息已更新：" + o);
                DataSource dataSource = DruidMain.getConf(zkClient, s);
                DruidMain.execQuery(dataSource);
            }

            @Override
            public void handleDataDeleted(String s) throws Exception {
                System.out.println(s + "配置信息不存在");
            }
        });

        // 手动更新zk节点里的配置信息，用来触发监听器
        zkClient.writeData(zkPath, "jdbc:mysql://localhost:3306/mysql?characterEncoding=UTF-8	root	wusheng");
        Thread.sleep(5000);


    }
}