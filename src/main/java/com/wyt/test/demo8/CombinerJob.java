package com.wyt.test.demo8;

import com.wyt.test.demo7.WorkCountSumJob;
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

public class CombinerJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        //run方法接受run中的返回1or0
        int run = ToolRunner.run(new CombinerJob(), args);
        System.out.println(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        //注册
        Configuration entries = new Configuration();
        entries.set("fs.defaultFS", "hdfs://192.168.10.104:9000");
        Job job = Job.getInstance(entries);
        //2.设置jar路径
        job.setJarByClass(CombinerJob.class);
        //3.关联map 关联reducer
        job.setMapperClass(CombinerJob.CombinerMapper.class);
        job.setReducerClass(CombinerJob.CombinerReducer.class);
        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setCombinerClass(CombinerJob.CombinerReducer.class);
        FileInputFormat.addInputPath(job, new Path("/mapreduce/demo8/combiner.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/mapreduce/demo8/out"));
        //7.提交job 执行作业 并且 等待执行结束 run方法 返回值是int  1就是成功  0 就是失败
        return job.waitForCompletion(true) ? 1 : 0;
    }

    static class CombinerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        IntWritable one = new IntWritable();
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
            String[] split = line.split("\t");
            text.set(split[0]);
            one.set(Integer.parseInt(split[1]));
            context.write(text, one);
        }
    }

    static class CombinerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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