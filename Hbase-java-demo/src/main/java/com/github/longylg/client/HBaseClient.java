package com.github.longylg.client;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.MobCompactPartitionPolicy;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class HBaseClient {

    private Connection conn = null;

    public HBaseClient(String host, String clientPort) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", host);
        configuration.set("hbase.zookeeper.property.clientPort", clientPort);
        configuration.addResource(new Path("/home/liyulong/IdeaProjects/HbaseHDFSDemo/Hbase-java-demo/src/main/resources/hbase-site.xml"));
        HBaseAdmin.available(configuration);
        conn = ConnectionFactory.createConnection(configuration);
    }


    public void listTable() throws IOException {
        System.out.println("开始打印HBase table name...");
        HTableDescriptor[] hTableDescriptors = conn.getAdmin().listTables();
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            System.out.println(hTableDescriptor.getTableName());
        }

    }

    public void listColumnFamilies(String tableName) throws IOException {
        System.out.println("开始打印 table[test] column Families...");

        ColumnFamilyDescriptor[] columnFamilyDescriptors = conn.getTable(TableName.valueOf(tableName)).getDescriptor().getColumnFamilies();

        for (ColumnFamilyDescriptor columnFamilyDescriptor : columnFamilyDescriptors) {
            System.out.println("name:" + columnFamilyDescriptor.getNameAsString());
            System.out.println("size:" + columnFamilyDescriptor.getBlocksize());
            columnFamilyDescriptor.getValues().forEach((k, v) -> System.out.println(k + "||" + v));
        }

        System.out.println("******************** 打印第一个列族 key-value ***********************");
        ResultScanner scanner = conn.getTable(TableName.valueOf(tableName)).getScanner(columnFamilyDescriptors[0].getName());
        Iterator<Result> it = scanner.iterator();
        if (it.hasNext()) {
            Result result = it.next();
            result.getFamilyMap(columnFamilyDescriptors[0].getName()).forEach((k, v) -> {
                System.out.println(Bytes.toString(k));
                System.out.println(Bytes.toString(v));
            });
        }
    }

    public String getValue(String tableName, String rowkey, String family, String quelifier) throws IOException {
        Get get = new Get(rowkey.getBytes());
        Result result = conn.getTable(TableName.valueOf(tableName)).get(get);

        return Bytes.toString(result.getValue(family.getBytes(), quelifier.getBytes()));
    }

    public void createTableWithMOB(String tableName, String[] columnsFamily) throws IOException {
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        for (String s : columnsFamily) {
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.of(s);
            ColumnFamilyDescriptorBuilder.newBuilder(columnFamilyDescriptor)
                                         .setMobEnabled(true)
                                         .setMobThreshold(10240L)
                                         .setMobCompactPartitionPolicy(MobCompactPartitionPolicy.DAILY);

            builder.setColumnFamily(columnFamilyDescriptor);
        }
        conn.getAdmin().createTable(builder.build());
    }

    public void createTable(String tableName, String[] columnsFamily) throws IOException {
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        for (String s : columnsFamily) {
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.of(s);
            ColumnFamilyDescriptorBuilder.newBuilder(columnFamilyDescriptor);

            builder.setColumnFamily(columnFamilyDescriptor);
        }
        conn.getAdmin().createTable(builder.build());
    }

    public void putFileData(String rowKey, String tableName, String family, Map<String, String> MetaData, byte[] file) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));
        MetaData.forEach((k, v) -> put.addColumn(Bytes.toBytes(family), Bytes.toBytes(k), Bytes.toBytes(v)));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes("blob"), file);
        conn.getTable(TableName.valueOf(tableName)).put(put);
    }


}
