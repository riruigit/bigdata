package com.lingnan.mr;

import com.lingnan.convertor.DimensionConvertorImpl;
import com.lingnan.kv.BaseDimension;
import com.lingnan.kv.CommDimension;
import com.lingnan.kv.CountDurationValue;
import com.lingnan.util.JDBCInstance;
import com.lingnan.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLOutPutFormat extends OutputFormat<BaseDimension, CountDurationValue> {

    private FileOutputCommitter committer = null;
    @Override
    public RecordWriter<BaseDimension, CountDurationValue> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Connection connection = null;
        try {
            connection = JDBCInstance.getConnection();
//            关闭自动提交
            connection.setAutoCommit(false);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return new MySqlRecordWriter(connection);
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

    static class MySqlRecordWriter extends RecordWriter<BaseDimension, CountDurationValue>{

        private Connection connection = null;
        private PreparedStatement preparedStatement = null;
//        设置sql缓存条数边界
        private int batchBound = 500;
//        设置已经缓存的条数
        private int batchSize = 0;

        public MySqlRecordWriter(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void write(BaseDimension baseDimension, CountDurationValue countDurationValue) throws IOException, InterruptedException {
//从数据拿到的也是CommDimension类型
            CommDimension commDimension = (CommDimension) baseDimension;
            String sql ="INSERT INTO `tb_call` VALUE (?,?,?,?,?) ON DUPLICATE KEY UPDATE `call_sum` = ?,`call_duration_sum` = ?;";

            DimensionConvertorImpl convertor = new DimensionConvertorImpl();

            int contactId = convertor.getDimensionID(commDimension.getContactDimension());
            int dateId = convertor.getDimensionID(commDimension.getDateDimension());

            String date_contact = dateId+"_"+contactId;

            int countSum = Integer.valueOf(countDurationValue.getCountSum());
            int durationSum = Integer.valueOf(countDurationValue.getDurationSum());

            try {
                preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setString(1,date_contact);
                preparedStatement.setInt(2,dateId);
                preparedStatement.setInt(3,contactId);
                preparedStatement.setInt(4,countSum);
                preparedStatement.setInt(5,durationSum);
                preparedStatement.setInt(6,countSum);
                preparedStatement.setInt(7,durationSum);
                //将sql缓存到客户端
                preparedStatement.addBatch();
                batchSize++;
                if(batchSize >= batchBound){
//                    批量执行sql语句
                    preparedStatement.executeBatch();
                    connection.commit();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            try {
                if(preparedStatement != null){
                    preparedStatement.executeBatch();
                    connection.commit();

                }
                JDBCUtil.close(connection,preparedStatement,null);

            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }
}
