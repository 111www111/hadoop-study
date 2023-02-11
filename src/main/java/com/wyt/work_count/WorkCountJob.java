package com.wyt.work_count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.lang.model.SourceVersion;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;


public class WorkCountJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        //run方法接受run中的返回1or0
        int run = ToolRunner.run(new WorkCountJob(), args);
        System.out.println(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        //注册
        Configuration entries = new Configuration();
        entries.set("fs.defaultFS","hdfs://192.168.10.104:9000");
        Job job = Job.getInstance(entries);
        //2.设置jar路径
        job.setJarByClass(WorkCountJob.class);
        //3.关联map 关联reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("/111.txt"));
        FileOutputFormat.setOutputPath(job,new Path("/out"));
        //7.提交job 执行作业 并且 等待执行结束 run方法 返回值是int  1就是成功  0 就是失败
        return job.waitForCompletion(true)?1:0;
    }

    /***
     * Mapper泛型为输入的KV和输出的KV格式
     */
    public static class WordCountMapper extends Mapper<LongWritable,Text, Text, IntWritable>{
        IntWritable one = new IntWritable(1);
        Text text = new Text();
        /**
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split(",");
            for (String work:split) {
                text.set(work);
                context.write(text,one);
            }
        }
    }

    /**
     * reducer 泛型为输入输出
     */
    public static class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable>{
        IntWritable intWritable = new IntWritable(1);
        Text text = new Text();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value:values) {
                sum = sum + value.get();
            }
            intWritable.set(sum);
            text.set(key);
            context.write(text,intWritable);
        }
    }
}
