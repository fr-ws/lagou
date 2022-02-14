package com.lagou.homework;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MyProcessor extends BaseRegionObserver {
//    Logger logger = LoggerFactory.getLogger(HbaseClientDemoclass);

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        HTableInterface relations = e.getEnvironment().getTable(TableName.valueOf("user_relations"));
        byte[] row = delete.getRow();
        Set<Map.Entry<byte[], List<Cell>>> entries = delete.getFamilyCellMap().entrySet();
        for (Map.Entry<byte[], List<Cell>> entry : entries) {
            System.out.println("------------" + Bytes.toString(entry.getKey()));
            List<Cell> values = entry.getValue();
            for (Cell value : values) {
                byte[] rowkey = CellUtil.cloneRow(value);
                byte[] column = CellUtil.cloneQualifier(value);
//                判断要删除的数据是否存在
                boolean flag = relations.exists(new Get(column).addColumn(Bytes.toBytes("friends"),rowkey));
                if (flag){
                    Delete deleteF = new Delete(column).addColumn(Bytes.toBytes("friends"), rowkey);
                    relations.delete(deleteF);
                }
            }
        }
        relations.close();
    }
}

