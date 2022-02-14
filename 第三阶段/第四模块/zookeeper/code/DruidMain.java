package com.lagou.zk.demo;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.I0Itec.zkclient.ZkClient;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class DruidMain {

    /**
     * 获取zk节点中的数据库配置信息并加载配置信息
     * @param path zk节点路径
     * @param zkClient zk客户端连接
     */
    public static DataSource getConf(ZkClient zkClient , String path){
//        ZkUtils.createNode(zkClient,path);
        String dbConfig = zkClient.readData(path);
        String url = dbConfig.split("\t")[0];
        String username = dbConfig.split("\t")[1];
        String password = dbConfig.split("\t")[2];

        DataSource dataSource = null;
        try {
            Map<String,String> dbConfigMap = new HashMap<String,String>();
            dbConfigMap.put("url",url);
            dbConfigMap.put("username",username);
            dbConfigMap.put("password",password);
            dataSource = DruidDataSourceFactory.createDataSource(dbConfigMap);
        }  catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return dataSource;
    }


    //    获取连接
    public static Connection getConnection(DataSource dataSource){
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }


    //    释放资源
    public static void close(Connection con, Statement statement, ResultSet resultSet){
        if (con != null && statement !=null && resultSet !=null){
            try {
                resultSet.close();
                statement.close();
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    //查询数据库信息，测试连接
    public static void execQuery(DataSource dataSource){
        Connection con = DruidMain.getConnection(dataSource);
        System.out.println("连接成功，数据库列表如下：");
        Statement statement = null;
        ResultSet re = null;
        try {
            statement = con.createStatement();
            re = statement.executeQuery("show databases");
            while (re.next()){
               System.out.println(re.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        DruidMain.close(con,statement,re);
    }




}
