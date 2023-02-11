package com.wyt.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

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
        configuration.set("dfs.replication", "2");
        configuration.set("fs.defaultFS","hdfs://192.168.10.104:9000");
        try {
            //获取客户端对象
            fsClient = FileSystem.get(
                    //NameNode的通讯URI
                    new URI("hdfs://192.168.10.104:9000"),
                    //配置文件
                    configuration,
                    //设置用户
                    "root"
            );
        } catch (Exception e) {
            System.out.println(e + e.getMessage());
        }

    }

    /**
     * 封装关闭资源
     */
    @After
    public void close() {
        //关闭资源
        if (fsClient != null) {
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
                false,
                true,
                new Path("C:\\Users\\13169\\Desktop\\input\\111.txt"),
                new Path("/"));
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

    /**
     * 删除
     */
    @Test
    public void testRm() throws Exception {
        // path:路径，b:是否递归
        //当然，如果你这里路径下还有文件且不是递归，会报错
        fsClient.delete(new Path("/tmp"), true);
    }

    /**
     * 文件的重命名与移动
     */
    @Test
    public void testMv() throws IOException {
        //源文件路径，目标文件路径
        fsClient.rename(
                new Path("/sanguo/shuguo1.txt"),
                new Path("/sanguo/shuguo.txt")
        );
    }

    /**
     * 获取文件详情
     */
    @Test
    public void getFileList() throws IOException {
        //获取所有文件信息
        // 路径，是否递归
        RemoteIterator<LocatedFileStatus> fileList = fsClient.listFiles(new Path("/"), true);
        //遍历迭代器
        while (fileList.hasNext()) {
            LocatedFileStatus next = fileList.next();
            System.out.println("----------------------");
            System.out.println("路径：" + next.getPath());
            System.out.println("权限：" + next.getPermission());
            System.out.println("用户：" + next.getOwner());
            System.out.println("分组：" + next.getGroup());
            System.out.println("大小：" + next.getLen());
            System.out.println("时间" + next.getModificationTime());
            System.out.println("副本：" + next.getReplication());
            System.out.println("块大小：" + next.getBlockSize());
            System.out.println("文件名称：" + next.getPath().getName());
            System.out.println("（块）存储位置：" + Arrays.toString(next.getBlockLocations()));
        }
    }

    /**
     * 判断是文件夹还是文件
     */
    @Test
    public void testFile() throws IOException {
        FileStatus[] fileStatuses = fsClient.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()){
                System.out.println(fileStatus.getPath().getName());
            }
        }
    }
}
