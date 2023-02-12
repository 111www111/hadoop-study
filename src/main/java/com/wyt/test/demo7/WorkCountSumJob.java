package com.wyt.test.demo7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class WorkCountSumJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        //run方法接受run中的返回1or0
        int run = ToolRunner.run(new WorkCountSumJob(), args);
        System.out.println(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        //注册
        Configuration entries = new Configuration();
        entries.set("fs.defaultFS", "hdfs://192.168.10.104:9000");
        Job job = Job.getInstance(entries);
        //2.设置jar路径
        job.setJarByClass(WorkCountSumJob.class);
        //3.关联map 关联reducer
        job.setMapperClass(WordCountSumMapper.class);
        job.setReducerClass(WordCountSumReducer.class);
        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //6.设置输入路径文件夹和输出路径,读取方式
//        job.setInputFormatClass(TextInputFormat.class);
        //这里设置合并小文件
        job.setInputFormatClass(CombineTextInputFormat.class);
        //合并小文件，后面是字节数 10*1024*1024 10M 10485760 ，当输入的文件大小不足这个大小，会将多个block 合并为一个切片split
        CombineTextInputFormat.setMaxInputSplitSize(job, 10485760);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("/mapreduce/demo7/"));
        FileOutputFormat.setOutputPath(job, new Path("/mapreduce/demo7/out"));
        //7.提交job 执行作业 并且 等待执行结束 run方法 返回值是int  1就是成功  0 就是失败
        return job.waitForCompletion(true) ? 1 : 0;
    }

    /***
     * Mapper泛型为输入的KV和输出的KV格式
     */
    public static class WordCountSumMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        IntWritable one = new IntWritable(1);
        Text text = new Text();

        /**
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split(" ");
            for (String work : split) {
                text.set(work);
                context.write(text, one);
            }
        }
    }

    /**
     * reducer 泛型为输入输出
     */
    public static class WordCountSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable intWritable = new IntWritable(1);
        Text text = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum = sum + value.get();
            }
            intWritable.set(sum);
            text.set(key);
            context.write(text, intWritable);
        }
    }
}
