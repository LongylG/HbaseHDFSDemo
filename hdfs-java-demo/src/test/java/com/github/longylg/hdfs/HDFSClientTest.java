package com.github.longylg.hdfs;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

public class HDFSClientTest {

    HDFSClient hdfsClient;

    @Before
    public void init() throws InterruptedException, IOException, URISyntaxException {
        hdfsClient = new HDFSClient("hdfs://docker:8020", "root");
    }

    @Test
    public void exist() throws IOException {
        System.out.println(hdfsClient.exist("test1.xml"));
    }

    @Test
    public void upload() {
        try (InputStream in = new FileInputStream("/home/liyulong/Desktop/test.xml");
             ByteArrayOutputStream byteOs = new ByteArrayOutputStream()) {
            byte[] buf = new byte[4096];
            while (in.read(buf) != -1) {
                byteOs.write(buf);
            }
            System.out.println(hdfsClient.upload(byteOs.toByteArray(), "/test1.xml"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}