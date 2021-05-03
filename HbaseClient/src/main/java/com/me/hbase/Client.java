package com.me.hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class Client {
    private static TableName student2 = TableName.valueOf("student2");
    private Connection hbaseConnection;

    @Before
    public void before() throws IOException {
        //1. new对象
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        hbaseConnection = ConnectionFactory.createConnection(configuration);
    }

    @After
    public void after() throws IOException {
        hbaseConnection.close();
    }

    @Test
    public void createTable() throws IOException {
        //从连接获取管理对象
        Admin admin = hbaseConnection.getAdmin();

        //2. 操作
        //创建一个ColumnFamilyDescriptor
        ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder
                .newBuilder("info".getBytes(StandardCharsets.UTF_8))
                .build();

        //创建一个TableDescriptor
        TableDescriptor tableDescriptor = TableDescriptorBuilder
                .newBuilder(student2)
                .setColumnFamilies(Collections.singleton(columnFamilyDescriptor))
                .build();

        admin.createTable(tableDescriptor);
        //3. 关闭资源
        admin.close();
    }

    @Test
    public void put() throws IOException {
        //1. 获取Table对象
        Table table = hbaseConnection.getTable(student2);

        //2. 插入数据
        Put put = new Put("1001".getBytes(StandardCharsets.UTF_8));
        put.addColumn(
                "info".getBytes(StandardCharsets.UTF_8),
                "name".getBytes(StandardCharsets.UTF_8),
                "张三".getBytes(StandardCharsets.UTF_8)
        );
        put.addColumn(
                "info".getBytes(StandardCharsets.UTF_8),
                "age".getBytes(StandardCharsets.UTF_8),
                "11".getBytes(StandardCharsets.UTF_8)
        );
        put.addColumn(
                "info".getBytes(StandardCharsets.UTF_8),
                "gender".getBytes(StandardCharsets.UTF_8),
                "男".getBytes(StandardCharsets.UTF_8)
        );
        table.put(put);

        //3. 关闭表格
        table.close();
    }

    @Test
    public void scan() throws IOException {
        //1. 获取table对象
        Table table = hbaseConnection.getTable(student2);

        //2. 查数据
        Scan scan = new Scan();

        scan.withStartRow("1001".getBytes(StandardCharsets.UTF_8));
        scan.withStopRow("1002".getBytes(StandardCharsets.UTF_8));

        ResultScanner results = table.getScanner(scan);

        //results是多行数据，迭代results：
        for (Result result : results) {
            showResult(result);
        }

        //3. 关闭Table对象
        table.close();

    }

    @Test
    public void get() throws IOException {
        //1. new table对象
        Table table = hbaseConnection.getTable(student2);

        //2. 查找数据
        Get get = new Get("1001".getBytes(StandardCharsets.UTF_8));
        get.addColumn(
                "info".getBytes(StandardCharsets.UTF_8),
                "name".getBytes(StandardCharsets.UTF_8)
        );
        Result result = table.get(get);

        showResult(result);

        //3. 关闭table
        table.close();

    }

    //打印一行结果
    public void showResult(Result result) {
        //result表示一行数据
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            byte[] rowkey = CellUtil.cloneRow(cell);
            byte[] cf = CellUtil.cloneFamily(cell);
            byte[] cq = CellUtil.cloneQualifier(cell);
            long timestamp = cell.getTimestamp();
            byte[] value = CellUtil.cloneValue(cell);
            System.out.println(
                    new String(rowkey, StandardCharsets.UTF_8) + "," +
                            new String(cf, StandardCharsets.UTF_8) + ":" +
                            new String(cq, StandardCharsets.UTF_8) + "," +
                            timestamp + "," +
                            new String(value, StandardCharsets.UTF_8)
            );
        }
    }
}
