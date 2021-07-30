package untils;

import constants.Constant;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
//连接Hbase的单例
public class ConnectionInstance {

    private static Connection connection;

    public ConnectionInstance() {
    }

    public static Connection getConnection() throws IOException {
        if(connection == null || connection.isClosed()){
            connection = ConnectionFactory.createConnection(Constant.CONF);
        }
        return connection;

    }
}
