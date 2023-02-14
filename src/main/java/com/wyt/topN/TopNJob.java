package com.wyt.topN;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 按照xx条件排序,然后输出前N个
 * @ClassName TopNJob.java
 * @Description topN
 * @author WangYiTong
 * @createTime 2023-02-14 15:04
 **/
public class TopNJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new TopNJob(), args);
        System.out.println(run);
    }

    @Override
    public int run(String[] strings) throws Exception {
        //注册,这边写本地,不写hdfs了
        Job job = Job.getInstance();
        //设置Mapper和Reducer
        job.setMapperClass(TopNJob.TopNMapper.class);
        job.setReducerClass(TopNJob.TopNReducer.class);
        return 0;
    }

    static class TopNMapper extends Mapper {

    }

    static class TopNReducer extends Reducer {

    }
}
