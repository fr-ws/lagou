package com.lagou.homework;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseClient {
    Configuration conf=null;
    Connection conn=null;
    HBaseAdmin admin =null;
    @Before
    public void init () throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","linux121,linux122");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conn = ConnectionFactory.createConnection(conf);
    }
    @After
    public void destroy(){
        if(admin!=null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(conn !=null){
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void createTable() throws IOException {
        admin = (HBaseAdmin) conn.getAdmin();
//        创建表描述器
        HTableDescriptor relation = new HTableDescriptor(TableName.valueOf("user_relations"));
//        设置列族描述器
        relation.addFamily(new HColumnDescriptor("friends"));
//        执行操作
        admin.createTable(relation);
        System.out.println("user_relations表创建成功！！");
    }

    @Test
    public void putData() throws IOException {
//        获取一个表对象
        Table t = conn.getTable(TableName.valueOf("user_relations"));
//        设置rowkey
        Put put = new Put(Bytes.toBytes("uid1"));
        List<Put> puts = new ArrayList<Put>();
//        添加数据
        put.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid2"), Bytes.toBytes("uid2"));
        put.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid3"), Bytes.toBytes("uid3"));
        put.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid4"), Bytes.toBytes("uid4"));
        puts.add(put);
        //设置rowkey
        put = new Put(Bytes.toBytes("uid2"));
//        添加数据
        put.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid1"), Bytes.toBytes("uid1"));
        put.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid3"), Bytes.toBytes("uid3"));
        puts.add(put);
        //设置rowkey
        put = new Put(Bytes.toBytes("uid3"));
//        添加数据
        put.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid1"), Bytes.toBytes("uid1"));
        put.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid2"), Bytes.toBytes("uid2"));
        puts.add(put);
        //设置rowkey
        put = new Put(Bytes.toBytes("uid4"));
//        添加数据
        put.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid1"), Bytes.toBytes("uid1"));
        puts.add(put);
//       执行插入
        t.put(puts);
        System.out.println("数据插入成功!");
    }

    @Test
    public void scanAllData() throws IOException {
        HTable relation = (HTable) conn.getTable(TableName.valueOf("user_relations"));
        Scan scan = new Scan();
        ResultScanner results = relation.getScanner(scan);
        for (Result result : results) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                System.out.println(rowKey + " " +cf + " " + column + "  " + value);
            }
        }
    }


}