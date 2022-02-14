package com.lagou.zk.demo;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

/*
* 自定义序列化
*
 */

public class ZkStrSerializer implements ZkSerializer {
    //序列化数据
    public byte[] serialize(Object o) throws ZkMarshallingError {

        return String.valueOf(o).getBytes();
    }
    //反序列化
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        return new String(bytes);
    }
}





















