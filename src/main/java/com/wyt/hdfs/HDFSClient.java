package com.wyt.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 客户算代码常用套路
 * 1.获取客户端对象
 * 2.执行相关操作命令
 * 3.关闭资源
 * 例如HDFS zookeeper 都这样
 */
public class HDFSClient {

    private FileSystem fsClient;

    /**
     * 封装初始化
     */
    @Before
    public void init() {
        Configuration configuration = new Configuration();
        //设置副本数
        configuration.set("dfs.replication","2");
        try {
            //获取客户端对象
            fsClient = FileSystem.get(
                    //NameNode的通讯URI
                    new URI("hdfs://192.168.10.101:8020"),
                    //配置文件
                    configuration,
                    //设置用户
                    "root"
            );
        }catch (Exception e){
            System.out.println(e+e.getMessage());
        }

    }

    /**
     * 封装关闭资源
     */
    @After
    public void close(){
        //关闭资源
        if(fsClient!=null){
            try {
                fsClient.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 创建文件夹
     */
    @Test
    public void testMkdir() throws IOException {
        fsClient.mkdirs(new Path("/fromJava/javaApiTest/be"));
    }

    /**
     * 上传文件
     * 参数优先级-》客户端代码中设置的值 >ClassPath 下的用户自定义配置文件 >
     * 然后是服务器的自定义配置（xxx-site.xml）>服务器的默认配置（xxx-default.xml）
     */
    @Test
    public void inputFile() throws IOException {
        //boolean delSrc, -> 是否删除原始数据？
        // boolean overwrite, -> 上传到目标地址，但是这个文件存在了，是否覆盖
        // Path[] srcs, -> 元数据路径
        // Path dst -> hadoop目的路径
        fsClient.copyFromLocalFile(
                true,
                true,
                new Path("testFile.txt"),
                new Path("/fromJava"));
    }

    /**
     * 文件下载
     */
    @Test
    public void outputFile() throws IOException {
        //boolean delSrc, ->源文件是否删除
        // Path src, ->hdfs路径
        // Path dst, ->目标地址路径
        // boolean useRawLocalFileSystem) ->
        fsClient.copyToLocalFile(false,
                new Path("/fromJava/testFile.txt"),
                new Path("testFile.txt"),
                false
        );
    }

}
