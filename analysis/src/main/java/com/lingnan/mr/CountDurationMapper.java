package com.lingnan.mr;

import com.lingnan.kv.CommDimension;
import com.lingnan.kv.ContactDimension;
import com.lingnan.kv.DateDimension;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CountDurationMapper extends TableMapper<CommDimension, Text> {

    private Map<String,String> phoneName =  new HashMap<>();

    private Text v = new Text();
//    setup方法被MapReduce框架仅且执行一次，在执行Map任务前，进行相关变量或者资源的集中初始化工作。
//
//若是将资源初始化工作放在方法map()中，导致Mapper任务在解析每一行输入时都会进行资源初始化工作，导致重复，程序运行效率不高！
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        init();
    }

    private void init(){
        phoneName.put("14546682403","周伟业");
        phoneName.put("17567800948","林子明");
        phoneName.put("15077057946","庄静丽");
        phoneName.put("14526013645","陈小婷");
        phoneName.put("14496041839","张杨平");
        phoneName.put("13084471848","武冠轩");
        phoneName.put("19429951573","周泉江");
        phoneName.put("13374117844","余炜鹏");
        phoneName.put("16691824296","张灿云");
        phoneName.put("15871463970","许伟谊");
        phoneName.put("13077212752","郭锦贤");
        phoneName.put("17560232813","朱泽亮");
        phoneName.put("14154522844","吴羽静");
        phoneName.put("14392458687","张铉");
        phoneName.put("17063744459","蔡迪全");
        phoneName.put("18709447318","林炎秀");
        phoneName.put("19714859547","梁梦怡");
        phoneName.put("13847776108","刁慧彬");
        phoneName.put("15185056818","陈增宏");
        phoneName.put("13144648921","刘瑞雄");
        phoneName.put("14047846832","刘俊声");
        phoneName.put("14003952848","陈学斌");
        phoneName.put("18252989872","王纯淇");
        phoneName.put("13760602622","梁绍荣");
        phoneName.put("19721415296","梁成鑫");
        phoneName.put("16681236351","廖庭康");
        phoneName.put("16815508236","张华扬");
        phoneName.put("17254260021","熊欢");
        phoneName.put("16796980338","马晓华");
        phoneName.put("14351188022","梁夏天");
        phoneName.put("13038384658","叶学斌");
        phoneName.put("13002379979","麦亮森");
        phoneName.put("16687602733","陈文创");
        phoneName.put("13948870600","苏日锐");
        phoneName.put("16837661133","庄海富");
        phoneName.put("15752670115","黄宇康");
        phoneName.put("18657343498","张铭");
        phoneName.put("18698777995","杨晴晴");
        phoneName.put("14493972794","姚能燕");
        phoneName.put("13111837376","陈培凤");
        phoneName.put("16698164974","杜国仲");
        phoneName.put("19826588765","黄俊轩");
        phoneName.put("18258311048","邓伟键");
        phoneName.put("18597719744","叶钦帅");
        phoneName.put("19112753434","何俊霖");
        phoneName.put("16144545968","文嘉星");
        phoneName.put("14644492451","王程锋");
        phoneName.put("14811659633","邹子庆");
        phoneName.put("16030482345","邓校君");
        phoneName.put("13082473161","杨君胜");
        phoneName.put("13692850852","陈煜");
        phoneName.put("13493005702","罗锦坚");
        phoneName.put("19165498353","黄锦晖");
        phoneName.put("13748863098","吴文悦");
        phoneName.put("14594696590","李伟豪");
        phoneName.put("13276976683","李亮蓉");
        phoneName.put("19954319578","邱泽松");
        phoneName.put("18323588367","杨建民");
        phoneName.put("13425016199","杨泳志");

    }
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
//        从value获取rowKey  ,key也能获取key
        String rowKey = Bytes.toString(value.getRow());

        String[] rowKeys = rowKey.split("_");
//        05_19826588765_2020-06-16 23:01:20_17560232813_1_0534
        String flag = rowKeys[4];
        if("0".equals(flag)){
            return;
        }
        String call1 = rowKeys[1];
        String call2 = rowKeys[3];
        String duration = rowKeys[5];
//        设置输出的值
        v.set(duration);

        String buildTime = rowKeys[2];
        String year = buildTime.substring(0, 4);
        String month = buildTime.substring(5, 7);
        String day = buildTime.substring(8, 10);

        CommDimension commDimension = new CommDimension();
//当一条数据插进来的时候就会写出6条不同维度的数据
//        第一个联系人
        ContactDimension contactDimension = new ContactDimension();
        contactDimension.setName(phoneName.get(call1));
        contactDimension.setPhoneNum(call1);

//      年维度
        DateDimension yearDimension = new DateDimension(year,"-1","-1");
        commDimension.setContactDimension(contactDimension);
        commDimension.setDateDimension(yearDimension);
        context.write(commDimension,v);

//      月维度
        DateDimension monthDimension = new DateDimension(year,month,"-1");
        commDimension.setDateDimension(monthDimension);
        context.write(commDimension,v);

//      日维度
        DateDimension dayDimension = new DateDimension(year,month,day);
        commDimension.setDateDimension(dayDimension);
        context.write(commDimension,v);

//        第二个联系人
        contactDimension.setName(phoneName.get(call2));
        contactDimension.setPhoneNum(call2);

//      年维度
        commDimension.setContactDimension(contactDimension);
        commDimension.setDateDimension(yearDimension);
        context.write(commDimension,v);

//      月维度
        commDimension.setDateDimension(monthDimension);
        context.write(commDimension,v);

//      日维度
        commDimension.setDateDimension(dayDimension);
        context.write(commDimension,v);




    }
}
