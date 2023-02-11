package com.wyt.test.demo2;

import com.wyt.work_count.WorkCountJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class PhoneFlowJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new PhoneFlowJob(), args);
        System.out.println(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        //注册
        Configuration entries = new Configuration();
        entries.set("fs.defaultFS","hdfs://192.168.10.104:9000");
        Job job = Job.getInstance(entries);
        //2.设置jar路径
        job.setJarByClass(PhoneFlowJob.class);
        //3.关联map 关联reducer
        job.setMapperClass(PhoneFlowJob.PhoneFlowMapper.class);
        job.setReducerClass(PhoneFlowJob.PhoneFlowReducer.class);
        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Phone.class);
        //5.设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("/mapreduce/demo2/phone.log"));
        FileOutputFormat.setOutputPath(job,new Path("/mapreduce/demo2/out"));
        //7.提交job 执行作业 并且 等待执行结束 run方法 返回值是int  1就是成功  0 就是失败
        return job.waitForCompletion(true)?1:0;
    }

    /**
     * map
     */
    static class PhoneFlowMapper extends Mapper<LongWritable, Text,
            Text, Phone> {
        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, Text, Phone>.Context context) throws IOException, InterruptedException {
            //key -> 偏移量
            //Value：1363157985066	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	24	27	2481	24681	200
            //输入Key -》 1363157985066电话号
            //输出value ：Phone内容
            String line = value.toString();
            String[] lineDatas = line.split("\t");
            String phoneNumber = lineDatas[1];
            String up = lineDatas[6];
            String down = lineDatas[7];
            Text t = new Text(phoneNumber);
            Phone phone = new Phone(Integer.parseInt(up), Integer.parseInt(down));
            context.write(t, phone);
        }
    }

    /**
     * reducer
     */
    static class PhoneFlowReducer extends Reducer<Text, Phone,
            Text, Text> {
        //输入key 电话号
        //输入value  phone【】
        //输出Key 手机号
        //输出value 上传流量:264  下载流量:3024  总数据流量:  3288
        @Override
        protected void reduce(Text key, Iterable<Phone> values, Reducer<Text, Phone, Text, Text>.Context context) throws IOException, InterruptedException {
            //key 是固定的 可能有多条数据
            int sunUp = 0;
            int sumDown = 0;
            for (Phone phone : values) {
                sunUp += phone.getUp();
                sumDown += phone.getDown();
            }
            Text text = new Text("上传流量:" + sunUp + "下载流量:" + sumDown + "  总数据流量:" + (sunUp + sumDown));
            context.write(key, text);
        }
    }
}
