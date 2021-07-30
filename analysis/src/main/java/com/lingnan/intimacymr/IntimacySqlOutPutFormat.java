package com.lingnan.intimacymr;

import com.lingnan.kv.CountDurationValue;
import com.lingnan.util.JDBCInstance;
import com.lingnan.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

public class IntimacySqlOutPutFormat extends OutputFormat<Text, CountDurationValue> {

    private FileOutputCommitter committer = null;
    @Override
    public RecordWriter<Text, CountDurationValue> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Connection connection = null;
        try {
            connection = JDBCInstance.getConnection();
//            关闭自动提交
//            connection.setAutoCommit(false);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return new IntimacySqlRecordWriter(connection);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if (this.committer == null) {
            Path output = getOutputPath(taskAttemptContext);
            this.committer = new FileOutputCommitter(output,taskAttemptContext);
        }

        return this.committer;
    }


    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir");
        return name == null ? null : new Path(name);
    }





    static class IntimacySqlRecordWriter extends RecordWriter<Text, CountDurationValue>{

        private Connection connection = null;
        private PreparedStatement preparedStatement = null;

        public IntimacySqlRecordWriter(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void write(Text text, CountDurationValue countDurationValue) throws IOException, InterruptedException {
            String contactTow = text.toString();
            String[] contactSplit = contactTow.split("_");
            String contactNum1 = contactSplit[0];
            String contactNum2 = contactSplit[1];

//           查找到用户
            GetContactIdByNum getContactIdByNum = new GetContactIdByNum();
            Integer id1 = getContactIdByNum.getId(contactNum1);
            Integer id2 = getContactIdByNum.getId(contactNum2);

//          插入数据前先查找有没有相同的两个电话号码对应的联系人id
            boolean existNumTow = getContactIdByNum.isExistNumTow(connection,id1, id2);



//
            try {
                if(!existNumTow){
//                    如果没有则插入
                     preparedStatement = connection.prepareStatement(
                            "INSERT INTO `tb_intimacy` (`contact_id1`,`contact_id2`,`call_count`,`call_duration_count`,`intimacy_rank`)" +
                                    " VALUE (?,?,?,?,?);");

                    Integer conTactAllSum = Integer.valueOf(countDurationValue.getCountSum());
                    Integer durationAllCount = Integer.valueOf(countDurationValue.getDurationSum());
                    double v = (conTactAllSum * 100 + durationAllCount) * 0.2;
                    int intimacy = (int) v;
                    preparedStatement.setInt(1,id1);
                     preparedStatement.setInt(2,id2);
                     preparedStatement.setInt(3,Integer.valueOf(countDurationValue.getCountSum()));
                     preparedStatement.setInt(4,Integer.valueOf(countDurationValue.getDurationSum()));
                     preparedStatement.setInt(5,intimacy);
                     preparedStatement.executeUpdate();
                }else {
//                    如果存在了哪存在的是哪个
                    //如果两个电话号码已经存在了则更新 加之前的时间和次数 （先查那个数据顺序是已经存在的）
//                    判断当前电话号码的正序是否存在
                    boolean existNumOne = getContactIdByNum.isExistNumOne(connection, id1, id2);
                    if(existNumOne){
                        Map<String, Integer> sumAndDuration = getContactIdByNum.findSumAndDuration(connection, id1, id2);
                        Integer sum = sumAndDuration.get("sum");
                        Integer duration = sumAndDuration.get("duration");
                        int contactSum = Integer.valueOf(countDurationValue.getCountSum()) + sum;
                        int durationSum = Integer.valueOf(countDurationValue.getDurationSum()) + duration;
                        double v = (contactSum * 100 + durationSum) * 0.2;
                        int intimacy = (int) v;
                        preparedStatement = connection.prepareStatement(
                                "UPDATE `tb_intimacy` SET `intimacy_rank` = ?, `call_count` = ?,`call_duration_count` = ? WHERE `contact_id1` = ? AND `contact_id2` =?;");
                        preparedStatement.setInt(1,intimacy);
                        preparedStatement.setInt(2,contactSum);
                        preparedStatement.setInt(3,durationSum);
                        preparedStatement.setInt(4,id1);
                        preparedStatement.setInt(5,id2);
                        preparedStatement.executeUpdate();
                    }else {
//                        当前的id正序不存在,则只可能是反序存在
                        Map<String, Integer> sumAndDuration = getContactIdByNum.findSumAndDuration(connection, id2, id1);
                        Integer sum = sumAndDuration.get("sum");
                        Integer duration = sumAndDuration.get("duration");
                        int contactSum = Integer.valueOf(countDurationValue.getCountSum()) + sum;
                        int durationSum = Integer.valueOf(countDurationValue.getDurationSum()) + duration;
                        double v = (contactSum * 100 + durationSum) * 0.2;
                        int intimacy = (int) v;
                        preparedStatement = connection.prepareStatement(
                                "UPDATE `tb_intimacy` SET `intimacy_rank` = ?,`call_count` = ?,`call_duration_count` = ? WHERE `contact_id1` = ? AND `contact_id2` =?;");
                        preparedStatement.setInt(1,intimacy);
                        preparedStatement.setInt(2,contactSum);
                        preparedStatement.setInt(3,durationSum);
//                        这里的反着写
                        preparedStatement.setInt(4,id2);
                        preparedStatement.setInt(5,id1);
                        preparedStatement.executeUpdate();
                    }
                }

            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }



//           最后通过通话次数和通话时间做计算亲密度并更新数据


        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            JDBCUtil.close(connection,preparedStatement,null);
        }
    }
}
