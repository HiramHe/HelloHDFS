package cn.edu.hust;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;

public class HelloHDFS {

    public static void main (String[] args)  throws Exception{
        //readHDFSfile1();

        //HDFSfile1();

        //putWindowsFileToHDFS1();

        putWindowsFileToHDFS2();

        getFileInformationOfRootDir1();
    }

    public static void readHDFSfile1()  throws Exception{
        //URL默认只认识http协议
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

        URL url = new URL("hdfs://192.168.2.101:9000/hello.txt");
        InputStream in = url.openStream();
        IOUtils.copyBytes(in,System.out,4096,true);
    }


    //使用FileSystem
    public static FileSystem getFileSystem(String namenode,String replication) throws Exception{
        Configuration conf = new Configuration();
        //键值对
        conf.set("fs.defaultFS",namenode);
        conf.set("dfs.replication",replication);
        FileSystem fileSystem = FileSystem.get(conf);

        return fileSystem;
    }

    public static void HDFSfile1() throws Exception{

        FileSystem fileSystem=getFileSystem("hdfs://192.168.2.101:9000","2");

        //在hadoop文件系统的根目录下创建heHaiMing目录
        boolean success = fileSystem.mkdirs(new Path("/heHaiMing"));
        System.out.println(success);

        //判断一个文件是否存在;当新建一个目录或者文件时，先判断它是否存在
        success = fileSystem.exists(new Path("/hello.txt"));
        System.out.println(success);

        //删除一个文件,true=不将删除的文件放进回收站
        success = fileSystem.delete(new Path("/heHaiMin"),true);
        System.out.println(success);
    }

    public static void putWindowsFileToHDFS1() throws Exception{

        FileSystem fileSystem=getFileSystem("hdfs://192.168.2.101:9000","2");

        //overwrite=覆盖
        FSDataOutputStream out = fileSystem.create(new Path("/test.data"),true);
        FileInputStream fin = new FileInputStream("src/testFile/heHaiMing_profile.txt");
        IOUtils.copyBytes(fin,out,4096,true);
    }

    //用最原始的javaio的方式来读windows的文件;优势在于可以了解上传的进度，设置进度条
    public static void putWindowsFileToHDFS2() throws Exception{

        FileSystem fileSystem=getFileSystem("hdfs://192.168.2.101:9000","2");

        //手工往上传
        FSDataOutputStream out = fileSystem.create(new Path("/test2.data"),true);
        FileInputStream fin = new FileInputStream("src/testFile/heHaiMing_profile.txt");
        byte[] buffer =new byte[4096];

        int len = fin.read(buffer);
        while (len != -1){
            out.write(buffer,0,len);
            //读下一组
            len = fin.read(buffer);
        }
        fin.close();
        out.close();
    }

    public static void getFileInformationOfRootDir1() throws Exception{

        FileSystem fileSystem=getFileSystem("hdfs://192.168.2.101:9000","2");

        FileStatus[] statuses = fileSystem.listStatus(new Path("/"));
        for(FileStatus status : statuses){
            System.out.println(status.getPath());
            System.out.println(status.getPermission());
            System.out.println(status.getReplication());
        }
    }

}
