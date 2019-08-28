package com.github.longylg.client;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

public class HBaseClient {

    private Connection conn = null;

    public HBaseClient(String host, String clientPort) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", host);
        configuration.set("hbase.zookeeper.property.clientPort", clientPort);
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

    public void listColumnFamilies() throws IOException {
        System.out.println("开始打印 table[test] column Families...");

        ColumnFamilyDescriptor[] columnFamilyDescriptors = conn.getTable(TableName.valueOf("test")).getDescriptor().getColumnFamilies();

        for (ColumnFamilyDescriptor columnFamilyDescriptor : columnFamilyDescriptors) {
            System.out.println("name:" + columnFamilyDescriptor.getNameAsString());
            System.out.println("size:" + columnFamilyDescriptor.getBlocksize());
            columnFamilyDescriptor.getValues().forEach((k, v) -> System.out.println(k + "||" + v));
        }
    }

    public String getValue(String tableName, String rowkey, String family, String quelifier) throws IOException {
        Get get = new Get(rowkey.getBytes());
        Result result = conn.getTable(TableName.valueOf(tableName)).get(get);

        return Bytes.toString(result.getValue(family.getBytes(), quelifier.getBytes()));
    }

    public void createTable(String tableName, String[] columnsFamily) throws IOException {
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        for (String s : columnsFamily) {
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(s));
        }
        conn.getAdmin().createTable(builder.build());
    }

    public void putData(String rowKey, String tableName, String family, Map<String, String> values) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));
        values.forEach((k, v) -> put.addColumn(Bytes.toBytes(family), Bytes.toBytes(k), Bytes.toBytes(v)));
        conn.getTable(TableName.valueOf(tableName)).put(put);
    }

    public static void main(String[] args) {
        try {
//            HBaseClient client = new HBaseClient("localhost", "2181");
            HBaseClient client = new HBaseClient("172.17.0.2", "2181");
            client.listColumnFamilies();
            System.out.println(client.getValue("test", "1", "fs", "name"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
