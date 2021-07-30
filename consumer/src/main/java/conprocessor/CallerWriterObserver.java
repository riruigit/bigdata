package conprocessor;

import constants.Constant;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import untils.ConnectionInstance;
import untils.HbaseUtil;
import untils.PropertyUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

//协处理器
public class CallerWriterObserver extends BaseRegionObserver {

    private Integer regions = Integer.valueOf(PropertyUtil.properties.getProperty("hbase.regions"));
    private SimpleDateFormat sdf;

//  在post之后调用的方法
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
//        之前操作的表
        String tableName = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
        String curTableName = PropertyUtil.properties.getProperty("hbase.table.name");
        if(!tableName.equals(curTableName)) {
            return;
        }
//05_19954319578_2019-09-14 20:30 获取之前操作的RowKey
        String row = Bytes.toString(put.getRow());
        String[] rowSplit = row.split("_");
        String flag = rowSplit[4];
        //插入之后 插进去的为0的话就不需要继续插入了，否则一直循环插入一条数据
        if("0".equals(flag)) {
            return;
        }
        String caller = rowSplit[1];
        String buildTime = rowSplit[2];
        String callee = rowSplit[3];
        String duration = rowSplit[5];
        sdf = new SimpleDateFormat(("yyyy-MM-dd HH:mm:ss"));
        String buildTime_ts = null;
        try {
            buildTime_ts = String.valueOf(sdf.parse(buildTime).getTime());
        } catch (ParseException parseException) {
            parseException.printStackTrace();
        }
        String regionHash = HbaseUtil.getRegionHash(callee, buildTime, regions);
        String rowKey = HbaseUtil.getRowKey(regionHash, callee, buildTime, caller, "0", duration);

//       HBse 获取连接
        Connection connection = ConnectionInstance.getConnection();
        Table table = connection.getTable(TableName.valueOf(curTableName));

        Put newPut = new Put(Bytes.toBytes(rowKey));
        newPut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("call1"),Bytes.toBytes(callee));
        newPut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("call2"),Bytes.toBytes(caller));
        newPut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("buildTime"),Bytes.toBytes(buildTime));
        newPut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("buildTime_ts"),Bytes.toBytes(buildTime_ts));
        newPut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("flag"),Bytes.toBytes("0"));
        newPut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("duration"),Bytes.toBytes(duration));

        table.put(newPut);
        table.close();

    }
}
