package com.wyt.serialize;

import com.wyt.mapreducer.WordCountDriver;
import com.wyt.mapreducer.WordCountMapper;
import com.wyt.mapreducer.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 执行
 */
public class FlowDriver {
    public static void main(String[] args) throws Exception {
        //1.获取job
        Job job = Job.getInstance(new Configuration());
        //2.设置jar路径
        job.setJarByClass(FlowDriver.class);
        //3.关联map 关联reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //5.设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\13169\\Desktop\\in"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\13169\\Desktop\\out"));
        //7.提交job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
