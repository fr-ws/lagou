package com.lagou.zk.demo;

import org.I0Itec.zkclient.ZkClient;

public class ZkMain {
    private ZkClient zkClient = null;

    //获取zk连接
    public ZkClient connectZK() {
        zkClient = new ZkClient("linux121:2181,linux122:2181,linux123:2181");
//        if(!zkClient.exists("/dbconfig")){
//            zkClient.createPersistent("/dbconfig");
//        }
        return zkClient;
    }

    //注册MySQL配置参数信息到zk
    public void registerServerInfo(String config) {
        //创建临时顺序节点
        if (!zkClient.exists("/dbconfig/mysql")) {
            zkClient.createPersistent("/dbconfig/mysql", config);
            System.out.println("读取到配置信息：" + config);
        } else {
            zkClient.writeData("/dbconfig/mysql", config);
            System.out.println("数据库配置信息已更新：" + config);
        }
    }



}
