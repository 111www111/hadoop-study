package com.wyt.test.demo6;

import com.wyt.test.demo5.DescLongWritable;
import com.wyt.test.dome4.OrderByKeyJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

/**
 * 排序 倒叙
 */
public class DoubleOrderDescJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new DoubleOrderDescJob(), args);
        System.out.println(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        //注册
        Configuration entries = new Configuration();
        entries.set("fs.defaultFS","hdfs://192.168.10.104:9000");
        Job job = Job.getInstance(entries);
        //2.设置jar路径
        job.setJarByClass(DoubleOrderDescJob.class);
        //3.关联map 关联reducer
        job.setMapperClass(DoubleOrderDescJob.OrderByKeyMapper.class);
        job.setReducerClass(DoubleOrderDescJob.OrderByKeyReducer.class);
        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(Player.class);
        job.setMapOutputValueClass(Text.class);
        //5.这是reduce输入Kv
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Player.class);
        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("/mapreduce/demo6/sort3.txt"));
        FileOutputFormat.setOutputPath(job,new Path("/mapreduce/demo6/out"));
        //7.提交job 执行作业 并且 等待执行结束 run方法 返回值是int  1就是成功  0 就是失败
        return job.waitForCompletion(true)?1:0;
    }

    static class OrderByKeyMapper extends Mapper<LongWritable, Text,Player,Text> {
        Text text = new Text();
        Player longWritable = new Player();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Player, Text>.Context context) throws IOException, InterruptedException {
            String[] lineDatas = value.toString().split("\t");
            //xxx yyy
            longWritable.setPeopleNum(Integer.parseInt(lineDatas[1]));
            longWritable.setVideoTime(Integer.parseInt(lineDatas[2]));
            text.set(lineDatas[0]);
            context.write(longWritable,text);
        }
    }

    static class OrderByKeyReducer extends Reducer<Player, Text,Text,Player> {
        @Override
        protected void reduce(Player key, Iterable<Text> values, Reducer<Player, Text, Text, Player>.Context context) throws IOException, InterruptedException {
            for (Text name:values) {
                context.write(name,key);
            }
        }
    }
}
