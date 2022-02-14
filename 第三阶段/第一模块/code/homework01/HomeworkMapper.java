package com.lagou.mr.homework01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HomeworkMapper extends Mapper<LongWritable, Text, IntWritable,NullWritable> {
     IntWritable k =  new IntWritable();


     //重写map方法，将每一行的数字作为map任务输出的key，利用mr的sort
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        k.set(Integer.parseInt(line));
        context.write(k, NullWritable.get());

    }
}
