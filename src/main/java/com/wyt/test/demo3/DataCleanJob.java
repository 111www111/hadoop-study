package com.wyt.test.demo3;

import com.ctc.wstx.util.StringUtil;
import com.wyt.test.demo2.Phone;
import com.wyt.test.demo2.PhoneFlowJob;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Objects;

/**
 * 数据清洗
 */
public class DataCleanJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new DataCleanJob(), args);
        System.out.println(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        //注册
        Configuration entries = new Configuration();
        entries.set("fs.defaultFS","hdfs://192.168.10.104:9000");
        Job job = Job.getInstance(entries);
        //2.设置jar路径
        job.setJarByClass(DataCleanJob.class);
        //3.关联map 关联reducer
        job.setMapperClass(DataCleanJob.DataCleanMapper.class);
        job.setNumReduceTasks(0);
        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("/mapreduce/demo3/dataclean.log"));
        FileOutputFormat.setOutputPath(job,new Path("/mapreduce/demo3/out"));
        //7.提交job 执行作业 并且 等待执行结束 run方法 返回值是int  1就是成功  0 就是失败
        return job.waitForCompletion(true)?1:0;
    }


    /**
     * 这一步做清洗数据
     */
    static class DataCleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            context.getCounter("DataCleanJob","DataCleanJobSum").increment(1L);
            if (check(value.toString())) {
                context.write(value, NullWritable.get());
                context.getCounter("DataCleanJob","DataCleanJobTure").increment(1L);
            }else{
                context.getCounter("DataCleanJob","DataCleanJobFalse").increment(1L);
            }
        }

        private boolean check(String lineData) {
            if (StringUtils.isBlank(lineData)) {
                return false;
            }
            String[] lineDatas = lineData.split("\t");
            if (lineDatas.length != 9) {
                return false;
            }
            //手机号
            if (Objects.equals(lineDatas[1], "null") || StringUtils.isBlank(lineDatas[1])) {
                return false;
            }
            //上传流量
            if (Objects.equals(lineDatas[6], "null") || StringUtils.isBlank(lineDatas[1])) {
                return false;
            }
            //下载流量
            if (Objects.equals(lineDatas[7], "null") || StringUtils.isBlank(lineDatas[1])) {
                return false;
            }
            return true;
        }
    }
}
