import com.github.longylg.client.HBaseClient;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@RunWith(JUnit4.class)
public class ClientTest {

    HBaseClient client;

    @Before
    public void initClient() throws IOException {
//        client = new HBaseClient("localhost", "2181");
        client = new HBaseClient("dev03,dev02,dev01", "2181");
    }

    @Test
    public void createTable() throws IOException {
//        client.createTable("test3",new String[]{"fs"});
//        client.listTable();
        client.listColumnFamilies("test01");
    }


    @Test
    public void putFileData() throws IOException {


//            client.createTable("test3",new String[]{"fs"});
        //store file to hbase
        Map<String, String> map = new HashMap<>();
        map.put("name", "test.xml");
        FileInputStream in = new FileInputStream(new File("/home/liyulong/Desktop/test.xml"));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[1024];
        if (in.read(buf) != -1) {
            out.write(buf);
        }
        client.putFileData("1", "test3", "fs", map, out.toByteArray());

    }

    @Test
    public void getAndWriteFileToLocal() {
        try (FileOutputStream os = new FileOutputStream(new File("/home/liyulong/Desktop/111.xml"))) {
            byte[] blob = Bytes.toBytes(client.getValue("test", "1", "fs", "blob"));
            os.write(blob);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
