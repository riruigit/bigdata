package com.lingnan.convertor;

import com.lingnan.kv.BaseDimension;
import com.lingnan.kv.CommDimension;
import com.lingnan.kv.ContactDimension;
import com.lingnan.kv.DateDimension;
import com.lingnan.util.JDBCUtil;
import com.lingnan.util.LRUCache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

//获取时间和联系人维度对应的id
public class DimensionConvertorImpl implements IConvertor{

    LRUCache lruCache =  new LRUCache(3000);

    @Override
    public int getDimensionID(BaseDimension baseDimension) {
        //从缓存获取数据，有数据则返回数据
        String cacheKey = getCacheKey(baseDimension);
        if(lruCache.containsKey(cacheKey)){
             return lruCache.get(cacheKey);
        }

//      从mysql中是否有值，如果没有，插入数据
        String[] sqls = getSqls(baseDimension);

        Connection connection = JDBCUtil.getConnection();
//      执行sql   先查看mysql中是否有值，如果没有，插入数据，再获取相应的id值
        int id = -1;
        try {
            id = execSql(sqls,connection,baseDimension);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        if(id == -1) throw new RuntimeException("未匹配到相应的维度");

        lruCache.put(cacheKey,id);

        
        return id;
    }

//    如果查询成功则返回id，失败则返回-1
    private int execSql(String[] sqls,Connection connection,BaseDimension baseDimension) throws SQLException {

        int id = -1;
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sqls[0]);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
//      第一次查询 查询到则直接返回
        setArguments(preparedStatement,baseDimension);
        ResultSet resultSet = preparedStatement.executeQuery();
        if(resultSet.next()){
             return resultSet.getInt(1);
        }

//      查询不到则插入数据
        preparedStatement = connection.prepareStatement(sqls[1]);
        setArguments(preparedStatement,baseDimension);
        preparedStatement.executeUpdate();

//      第二次查询
        preparedStatement = connection.prepareStatement(sqls[0]);
        setArguments(preparedStatement,baseDimension);
        resultSet = preparedStatement.executeQuery();
        if(resultSet.next()){
            return resultSet.getInt(1);
        }

        return id;

    }

//    设置sql语句的参数 如果为时间维度的则插入三个参数，如果为联系人维度则插入两个参数
//    这个方法因为都是两个参数，所以插入查询都可以用这个方法设置参数
    private void setArguments(PreparedStatement preparedStatement,BaseDimension baseDimension) throws SQLException {
        if(baseDimension instanceof ContactDimension){
            ContactDimension contactDimension = (ContactDimension) baseDimension;
            preparedStatement.setString(1,contactDimension.getPhoneNum());
            preparedStatement.setString(2,contactDimension.getName());
        }else if (baseDimension instanceof DateDimension){
            DateDimension dateDimension = (DateDimension) baseDimension;
            preparedStatement.setInt(1,Integer.valueOf(dateDimension.getYear()));
            preparedStatement.setInt(2,Integer.valueOf(dateDimension.getMonth()));
            preparedStatement.setInt(3,Integer.valueOf(dateDimension.getDay()));
        }
    }

    private String[] getSqls(BaseDimension baseDimension){
        String[] sqls = new String[2];
        if(baseDimension instanceof ContactDimension){
            sqls[0] = "SELECT `id` FROM `tb_contacts` WHERE `telephone` = ? AND `name` = ?;";
            sqls[1] = "INSERT INTO `tb_contacts` VALUES(NULL,?,?);";
        }else if(baseDimension instanceof DateDimension){
            sqls[0] = "SELECT `id` FROM `tb_dimension_date` WHERE `YEAR` = ? AND `MONTH` = ? AND `DAY` = ? ;";
            sqls[1] = "INSERT INTO `tb_dimension_date` VALUES (NULL,?,?,?);";
        }
        return sqls;
    }

    private String getCacheKey(BaseDimension baseDimension){
        StringBuffer sb = new StringBuffer();
        if(baseDimension instanceof ContactDimension){
            ContactDimension contactDimension = (ContactDimension) baseDimension;
            sb.append(contactDimension.getPhoneNum());
        }else if(baseDimension instanceof DateDimension){
            DateDimension dateDimension = (DateDimension) baseDimension;
            sb.append(dateDimension.getYear()).append(dateDimension.getMonth()).append(dateDimension.getDay());
        }
        return sb.toString();
    }
}
