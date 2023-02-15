package com.wyt.homework.day01;

import com.wyt.test.demo3.DataCleanJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

/**
 * 获取商品平均时间
 */
public class ProductTimeAvg extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new ProductTimeAvg(), args);
        System.out.println(run);
    }

    /**
     * 求商品名字 商品的平均停留时间
     * 日期			域名				     商品URL路径					商品名       ID	    商品页的停留时间。
     * 	2020年3月3日	www.baizhiedu.com	/product/detail/10001.html	iphoneSE	10001	30
     */
    @Override
    public int run(String[] args) throws Exception {
        //注册
        Configuration entries = new Configuration();
        entries.set("fs.defaultFS","hdfs://192.168.10.104:9000");
        Job job = Job.getInstance(entries);
        //2.设置jar路径
        job.setJarByClass(ProductTimeAvg.class);
        //3.关联map 关联reducer
        job.setMapperClass(ProductTimeAvg.ProductTimeAvgMapper.class);
        job.setReducerClass(ProductTimeAvg.ProductTimeAvgReducer.class);
        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.reducer输出KV
        //5.设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("/home/day01/商品日志.txt"));
        FileOutputFormat.setOutputPath(job,new Path("/home/day01/out"));
        //7.提交job 执行作业 并且 等待执行结束 run方法 返回值是int  1就是成功  0 就是失败
        return job.waitForCompletion(true)?1:0;
    }

    static class ProductTimeAvgMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
        Text text = new Text();
        IntWritable intWritable = new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split("\t");
            if (split.length<5){
                return;
            }
            text.set(split[3]);
            intWritable.set(Integer.parseInt(split[5]));
            context.write(text,intWritable);
        }
    }

    static class ProductTimeAvgReducer extends Reducer<Text, IntWritable,Text, IntWritable>{
        IntWritable intWritable = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int count = 0;
            int sum = 0;
            for (IntWritable intWritable:values) {
                count++;
                sum+=intWritable.get();
            }
            intWritable.set(sum / count);
            context.write(key,intWritable);
        }
    }
}
