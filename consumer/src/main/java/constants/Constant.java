package constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

//获取HBase的配置 读取resource的配置
public class Constant {
    public static final Configuration CONF = HBaseConfiguration.create();

}
