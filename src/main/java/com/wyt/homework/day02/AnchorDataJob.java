package com.wyt.homework.day02;

import com.wyt.test.demo6.Player;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 0. 数据说明
 * 一周内的主播的直播日志数据：
 * 主播id  直播日期    热度    观看人数    本次直播打赏金额    本场直播观众观看总时长(每个人观看的时长总和)
 * 1. 将直播数据进行清洗
 * 清洗要求：
 * ① 保留如下字段
 * ② 将不合格的行数据过滤掉。
 * 保留：
 * 主播id  热度    观看人数    打赏金额 直播观众总时长。
 * 2. 打印有效数据和删除数据的个数，使用计数器
 * Map:
 * 输入数据=12
 * 有效数据=10
 * 无效数据=2
 * 3. 使用对象序列化
 * 统计数据，统计结果预期如下(主播id 总热度 总人数 总收入 平均观众观看时长(总时长/总人数))
 * 1001	234	100	23123	xxx
 */
public class AnchorDataJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new AnchorDataJob(), args);
        System.out.println(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        //注册
        Configuration entries = new Configuration();
        entries.set("fs.defaultFS","hdfs://192.168.10.104:9000");
        //路径
        Job job = Job.getInstance(entries);
        job.setJarByClass(AnchorDataJob.class);
        //关联map reducer
        job.setMapperClass(AnchorDataJobMapper.class);
        //本次不适用reducer
        job.setReducerClass(AnchorDataJobReducer.class);
        //设置Map输出
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(AnchorData.class);
        //设置最终输出
        job.setOutputKeyClass(AnchorData.class);
        job.setOutputValueClass(NullWritable.class);
        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("/home/day02/xx主播直播日志数据.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/home/day02/out"));
        return job.waitForCompletion(true) ? 1 : 0;
    }

    static class AnchorDataJobMapper extends Mapper<LongWritable, Text, IntWritable, AnchorData> {
        AnchorData data = new AnchorData();
        IntWritable intWritable = new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, AnchorData>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] lineData = line.split("\t");
            //总数
            context.getCounter("AnchorDataJob","输入数据").increment(1L);
            boolean check = this.check(lineData);
            if (check) {
                //通过，02345
                data.setId(Integer.valueOf(lineData[0]));
                data.setHeat(Long.valueOf(lineData[2]));
                data.setPeopleCount(Long.valueOf(lineData[3]));
                data.setMoney(Long.valueOf(lineData[4]));
                data.setPeopleTimeCount(Long.valueOf(lineData[5]));
                intWritable.set(data.getId());
                context.write(intWritable,data);
            }
            String msg = check?"有效数据":"无效数据";
            context.getCounter("AnchorDataJob",msg).increment(1L);

        }

        private boolean check(String[] lineData) {
            if (lineData.length != 6) {
                return false;
            }
            boolean isCheckOn = true;
            for (String data : lineData) {
                if (Objects.equals(data, "null") || Objects.isNull(data)) {
                    isCheckOn =false;
                }
            }
            return isCheckOn;
        }
    }

    static class AnchorDataJobReducer extends Reducer< IntWritable, AnchorData,AnchorData, NullWritable>{
        AnchorData data = new AnchorData();
        @Override
        protected void reduce(IntWritable key, Iterable<AnchorData> values, Reducer<IntWritable, AnchorData, AnchorData, NullWritable>.Context context) throws IOException, InterruptedException {
            //这里是同一个主播的流量数据，直接计算
            //总热度
            Long sumHeat = 0L;
            //总人数
            Long sumPeople = 0L;
            //总收入
            Long sumMoney = 0L;
            //平均时间长度总时长/总人数
            Long sumTime = 0L;
            for (AnchorData data:values) {
                sumHeat+=data.getHeat();
                sumPeople += data.getPeopleCount();
                sumMoney += data.getMoney();
                sumTime += data.getPeopleTimeCount();
            }
            data.setId(key.get());
            data.setHeat(sumHeat);
            data.setPeopleCount(sumPeople);
            data.setMoney(sumMoney);
            data.setPeopleTimeCount(Objects.equals(sumTime,0L)?0:sumPeople/sumTime);
            context.write(data,NullWritable.get());
        }
    }

}
