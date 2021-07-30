package dao;

import constants.Constant;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import untils.ConnectionInstance;
import untils.HbaseUtil;
import untils.PropertyUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class HbaseDao {
    private String nameSpace;
    private String tableName;
    private int regions;
    private String cf;
    private SimpleDateFormat sdf =null;
    private List<Put> putList;
    private HTable table;
    private String flag;
    private String cf2;

    public HbaseDao() throws IOException {
        
        nameSpace = PropertyUtil.properties.getProperty("hbase.namespace");
        tableName = PropertyUtil.properties.getProperty("hbase.table.name");
        regions = Integer.parseInt(PropertyUtil.properties.getProperty("hbase.regions"));
        cf = PropertyUtil.properties.getProperty("hbase.table.cf");
        cf2 = PropertyUtil.properties.getProperty("hbase.table.cf2");
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        putList = new ArrayList<>();
        flag="1";
        if(!HbaseUtil.isTableExist(tableName)){
            HbaseUtil.initNameSpace(nameSpace);
            HbaseUtil.createTable(tableName,regions,cf,cf2);
        }
    }

//    14496041839,19826588765,2020-08-02 15:30:53,1044
    public void Put(String ori) throws IOException, ParseException {
        if(putList.size() == 0){
//          单例模式创建连接
            Connection connection = ConnectionInstance.getConnection();
            table = (HTable)connection.getTable(TableName.valueOf(tableName));
//           取消自动提交 就可以不用每次都提交
            table.setAutoFlushTo(false);
            table.setWriteBufferSize(1024*1024);
        }


        String[] split = ori.split(",");

        String caller = split[0];
        String callee = split[1];
        String buildTime = split[2];
        String duration = split[3];
        long time = sdf.parse(buildTime).getTime();
        //转为字符串
        String buildTime_ts = String.valueOf(time);

        String regionHash = HbaseUtil.getRegionHash(caller, buildTime, regions);

        String rowKey = HbaseUtil.getRowKey(regionHash, caller, buildTime, callee, flag ,duration);

        Put put = new Put(Bytes.toBytes(rowKey));

        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("call1"),Bytes.toBytes(caller));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("call2"),Bytes.toBytes(callee));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("buildTime"),Bytes.toBytes(buildTime));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("buildTime_ts"),Bytes.toBytes(buildTime_ts));
//        插入一个flag
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("flag"),Bytes.toBytes(flag));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("duration"),Bytes.toBytes(duration));

        putList.add(put);
        //有20条数据就put并清空list 关闭表的连接
        if(putList.size()>20){
            table.put(putList);
            table.flushCommits();
            putList.clear();
            table.close();
        }


    }
}

