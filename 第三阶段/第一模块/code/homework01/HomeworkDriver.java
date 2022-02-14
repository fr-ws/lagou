package com.lagou.mr.homework01;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class HomeworkDriver  {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "HomeworkDriver");

        job.setJarByClass(HomeworkDriver.class);

        job.setMapperClass(HomeworkMapper.class);
        job.setReducerClass(HomeworkReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        deleteFile(new File("F:\\homeworkdata\\output"));

        FileInputFormat.setInputPaths(job,new Path("F:\\homeworkdata\\input"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\homeworkdata\\output"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);



    }


    /**
     * 先根遍历序递归删除文件夹
     *
     * @param dirFile 要被删除的文件或者目录
     * @return 删除成功返回true, 否则返回false
     */
    public static boolean deleteFile(File dirFile) {
        // 如果dir对应的文件不存在，则退出
        if (!dirFile.exists()) {
            return false;
        }

        if (dirFile.isFile()) {
            return dirFile.delete();
        } else {

            for (File file : dirFile.listFiles()) {
                deleteFile(file);
            }
        }

        return dirFile.delete();
    }
}
