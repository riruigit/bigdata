package untils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

//拿到kafka相关配置的工具类
public class PropertyUtil {
    public static Properties properties;

    static {
        InputStream resourceAsStream = ClassLoader.getSystemResourceAsStream("kafka-hbase.properties");
        try {
            properties = new Properties();
            properties.load(resourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
