package com.lingnan.intimacymr;

import com.lingnan.util.JDBCInstance;
import com.lingnan.util.JDBCUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class GetContactIdByNum {

    public Integer getId(String num){
        Connection connection = JDBCUtil.getConnection();
        PreparedStatement preparedStatement = null;
        int id = 0;
        try {
            preparedStatement = connection.prepareStatement(
                    "SELECT `id` FROM `tb_contacts` WHERE `telephone` = ?;");

            preparedStatement.setString(1,num);

            ResultSet resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                return resultSet.getInt(1);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return id;
    }

    public boolean isExistNumTow(Connection connection,Integer id1,Integer id2){
        PreparedStatement preparedStatement = null;
        Boolean flag = false;

        try {
             preparedStatement = connection.prepareStatement(
                    "SELECT `contact_id1`,`contact_id2` FROM `tb_intimacy` WHERE `contact_id1` = ? AND `contact_id2` =?;");
            preparedStatement.setInt(1,id1);
            preparedStatement.setInt(2,id2);
            ResultSet resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
               return true;
            }else {
                preparedStatement.setInt(1,id2);
                preparedStatement.setInt(2,id1);
                ResultSet resultSet2 = preparedStatement.executeQuery();
                if (resultSet2.next()){
                    return true;
                }
            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return flag;
    }

    public boolean isExistNumOne(Connection connection,Integer id1,Integer id2){
        PreparedStatement preparedStatement = null;
        Boolean flag = false;

        try {
             preparedStatement = connection.prepareStatement(
                    "SELECT `contact_id1`,`contact_id2` FROM `tb_intimacy` WHERE `contact_id1` = ? AND `contact_id2` =?;");
             preparedStatement.setInt(1,id1);
             preparedStatement.setInt(2,id2);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()){
                System.out.println("已经存在这两个号码");
                return true;
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return flag;
    }

//    找到对应的通话次数和通话时间
    public Map<String,Integer> findSumAndDuration(Connection connection,Integer id1,Integer id2){
        PreparedStatement preparedStatement = null;
        HashMap<String,Integer> map = new HashMap<>();
        int sum=0;
        int duration = 0;
        try {
           preparedStatement = connection.prepareStatement(
                    "SELECT `call_count`, `call_duration_count` FROM `tb_intimacy` WHERE `contact_id1` = ? AND `contact_id2` =?;");
           preparedStatement.setInt(1,id1);
           preparedStatement.setInt(2,id2);
            ResultSet resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                 sum = resultSet.getInt(1);
                duration = resultSet.getInt(2);
            }
            map.put("sum",sum);
            map.put("duration",duration);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return map;
    }

}
