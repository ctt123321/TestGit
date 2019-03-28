import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.net.URISyntaxException;

public class HDFSwrite {
    public static void main(String[] args) throws Exception {
        //获取环境变量
        Configuration conf = new Configuration();
        String url = "hdfs://192.168.159.129:9000";
        //生成文件系统
        FileSystem fs = FileSystem.get(new URI(url), conf, "hadoop");

        //创建目录
        Path path=new Path("/myhdfs");
        fs.mkdirs(path);

        fs.copyFromLocalFile(new Path("D://resource/ctt.txt"),new Path("/myhdfs/a.txt"));

        Path path1=new Path("/myhdfs/a.txt");
        FSDataInputStream fin = fs.open(path1);
        byte[] buff =new byte[512];
        int length=0;
        while ((length=fin.read(buff,0,512))!=-1)
            System.out.println(new String(buff,0,length));

        fs.close();


    }
}
