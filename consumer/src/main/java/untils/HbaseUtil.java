package untils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;

public class HbaseUtil {
    public static Configuration conf = HBaseConfiguration.create();

    //初始化命名空间
    public static void initNameSpace(String namespace) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();

        NamespaceDescriptor descriptor = NamespaceDescriptor.create(namespace).addConfiguration("create_ts", String.valueOf(System.currentTimeMillis())).build();
        admin.createNamespace(descriptor);
        close(connection,admin);
    }

    //判断表是否存在
    public static boolean isTableExist(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin =  connection.getAdmin();

        boolean tableExists = admin.tableExists(TableName.valueOf(tableName));
        close(connection,admin);
        return tableExists;
    }

    //创建表
    public static void createTable(String tableName,int regions,String... columnFamily) throws IOException {

        if (isTableExist(tableName)){
            System.out.println("表已存在");
            return;
        }
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();

        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));

        for (String column : columnFamily) {
            descriptor.addFamily(new HColumnDescriptor(column));
        }
        //添加一个协处理器用来加入被叫
        descriptor.addCoprocessor("conprocessor.CallerWriterObserver");
        //从配置文件读取regions的个数
//        Integer regions = Integer.valueOf(PropertyUtil.properties.getProperty("hbase.regions"));

        admin.createTable(descriptor,getSplitKeys(regions));
        close(connection,admin);
    }

//    设置预分区键
    private static byte[][] getSplitKeys(int regions){

        DecimalFormat format = new DecimalFormat("00");

        byte[][] splitKeys = new byte[regions][];

        for (int i = 0; i < regions; i++) {
            splitKeys[i] = Bytes.toBytes(format.format(i)+"|");
        }

        for (byte[] splitKey : splitKeys) {
            System.out.println(Bytes.toString(splitKey));
        }

        return splitKeys;
    }

    //生成行键
    public static String getRowKey(String regionHash,String caller,String buildTime,String callee,String flag,String duration){
//        regionHash为分区号
        return regionHash+"_"+caller+"_"+buildTime+"_"+callee+"_"+flag+"_"+duration;
    }

    public static String getRegionHash(String caller,String buildTime,int regions){
        int length = caller.length();
//获取手机号后4位
        String last4Num = caller.substring(length - 4);
//      获取年月
        String yearMonth = buildTime.replaceAll("-", "").substring(0, 6);
//      异或手机号、年月并模分区数
        int regionCode = (Integer.parseInt(last4Num) ^ Integer.parseInt(yearMonth)) % regions;

        DecimalFormat format = new DecimalFormat("00");
//      返回分区号
        return format.format(regionCode);
    }

//关闭资源
    public static void close(Connection connection, Admin admin, Table... tables) throws IOException {
        if(connection != null){
            connection.close();
        }
        if (admin != null){
            admin.close();
        }
        if(tables.length <= 0){
            return;
        }
        for (Table table : tables) {
            table.close();
        }
    }
}
