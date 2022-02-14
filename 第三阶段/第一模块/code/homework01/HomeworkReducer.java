package com.lagou.mr.homework01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HomeworkReducer extends Reducer<IntWritable, NullWritable,IntWritable,IntWritable> {
    int i = 0;
    IntWritable orderNum = new IntWritable();


    // 重写reduce方法，数据已经按key进行排序,存在重复数据，遍历输出序号和key
    @Override
    protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value : values) {
            i += 1;
            orderNum.set(i);
            context.write(orderNum,key);
        }

    }
}
