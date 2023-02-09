package com.wyt.work_count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class WorkCountJob {

    public static void main(String[] args) throws Exception {
        //注册
        Job job = Job.getInstance(new Configuration());
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
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\13169\\Desktop\\input"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\13169\\Desktop\\output"));
        //7.提交job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
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
