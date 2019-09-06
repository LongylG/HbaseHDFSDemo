package com.github.longylg.hdfs;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class HDFSClient {

    FileSystem fs;

    public HDFSClient(String url, String user) throws URISyntaxException, IOException, InterruptedException {
        HdfsConfiguration configuration = new HdfsConfiguration();
        fs = FileSystem.get(new URI(url), configuration, user);
    }

    public boolean exist(String path) throws IOException {
        boolean flag;
        flag = fs.exists(new Path(path));
        return flag;
    }

    public boolean upload(byte[] file, String storePath) {
        try {
            IOUtils.copyBytes(new ByteArrayInputStream(file), fs.create(new Path("test1.xml")), 4096, true);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }


}
