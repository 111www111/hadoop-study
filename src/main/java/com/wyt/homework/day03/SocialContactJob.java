package com.wyt.homework.day03;

import com.wyt.homework.day02.AnchorData;
import com.wyt.homework.day02.AnchorDataJob;
import org.apache.commons.lang3.StringUtils;
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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Objects;

/**
 * 社交日活数据
 * 统计每个小时内，访问的人数，按照小时升序排序
 */
public class SocialContactJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new SocialContactJob(), args);
        System.out.println(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        //注册
        Configuration entries = new Configuration();
        entries.set("fs.defaultFS","hdfs://192.168.10.104:9000");
        //路径
        Job job = Job.getInstance(entries);
        job.setJarByClass(SocialContactJob.class);
        //关联map reducer
        job.setMapperClass(SocialContactJob.SocialContactMapper.class);
        //本次不适用reducer
        job.setReducerClass(SocialContactJob.SocialContactReducer.class);
        //设置Map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置最终输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("/home/day03/xx社交媒体的日志数据.log"));
        FileOutputFormat.setOutputPath(job, new Path("/home/day03/out"));
        return job.waitForCompletion(true) ? 1 : 0;
    }

    static class SocialContactMapper extends Mapper<LongWritable, Text,Text,Text>{

        Text textDate = new Text();
        Text textIp = new Text();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            //获取数据
            String line = value.toString();
            if(this.check(line)){
                String[] split = line.split("``");
                String data = split[1];
                String[] dataAndRubbish = data.split("\"`(GET|POST)\"");
                //我们要的数据  ip和时间
                String ip = split[0];
                String dataTime = dataAndRubbish[0];
                //字符串转换Date
                Date date = null;
                DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH");
                try {
                    date=format.parse(dataTime);
                } catch (ParseException e) {
                    return;
                }
                //我们已经格式化好了 校验一下
                if(StringUtils.isBlank(date+"") || Objects.isNull(date)){
                    return;
                }
                textDate.set(data+"");
                textIp.set(ip);
                //校验完毕输出
                context.write(textDate,textIp);
            }
        }

        /**
         * 数据校验
         * @param line
         * @return
         */
        private boolean check(String line) {
            if(StringUtils.isBlank(line)){
                return false;
            }
            if (!line.contains("``")){
                return false;
            }
            String[] split = line.split("``");
            if(split.length!=3){
                return false;
            }
            //正则表达式 匹配 `GET 或者 `POST
//            if(!split[1].matches("^`(GET|POST)")){
//                return false;
//            }
            return true;
        }
    }

    static class SocialContactReducer extends Reducer<Text,Text,Text,Text>{
        Text text = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            //我们只需要统计ip的个数
            int count = 0;
            for (Text value : values) {
                count++;
            }
            text.set(count+"");
            context.write(key,text);
        }
    }
}
