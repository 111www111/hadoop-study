package com.wyt.test.dome4;

import com.wyt.test.demo3.DataCleanJob;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class OrderByKeyJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new OrderByKeyJob(), args);
        System.out.println(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        //注册
        Configuration entries = new Configuration();
        entries.set("fs.defaultFS","hdfs://192.168.10.104:9000");
        Job job = Job.getInstance(entries);
        //2.设置jar路径
        job.setJarByClass(OrderByKeyJob.class);
        //3.关联map 关联reducer
        job.setMapperClass(OrderByKeyJob.OrderByKeyMapper.class);
        job.setReducerClass(OrderByKeyJob.OrderByKeyReducer.class);
        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("/mapreduce/demo4/sort1.txt"));
        FileOutputFormat.setOutputPath(job,new Path("/mapreduce/demo4/out"));
        //7.提交job 执行作业 并且 等待执行结束 run方法 返回值是int  1就是成功  0 就是失败
        return job.waitForCompletion(true)?1:0;
    }

    static class OrderByKeyMapper extends Mapper<LongWritable, Text,LongWritable,Text>{
        Text text = new Text();
        LongWritable longWritable = new LongWritable();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
            String[] lineDatas = value.toString().split("\t");
            //xxx yyy
            longWritable.set(Long.parseLong(StringUtils.trim(lineDatas[1])));
            text.set(StringUtils.trim(lineDatas[0]));
            context.write(longWritable,text);
        }
    }

    static class OrderByKeyReducer extends Reducer<LongWritable, Text,Text,LongWritable>{
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            for (Text name:values) {
                context.write(name,key);
            }
        }
    }
}
