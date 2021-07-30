/**
 * @author 18364
 */

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Producer {

    //用于存放联系人电话
    private List<String> phoneNum = new ArrayList<>();

    //用于存放联系人电话和姓名的映射
    private Map<String, String> phoneName = new HashMap<>();

    private String start = "2019-01-01";
    private String end = "2021-01-07";

    /**
     * 初始化随机电话号码和姓名
     */
    public void intiPhone() {
        phoneNum.add("14546682403");
        phoneNum.add("17567800948");
        phoneNum.add("15077057946");
        phoneNum.add("14526013645");
        phoneNum.add("14496041839");
        phoneNum.add("13084471848");
        phoneNum.add("19429951573");
        phoneNum.add("13374117844");
        phoneNum.add("16691824296");
        phoneNum.add("15871463970");
        phoneNum.add("13077212752");
        phoneNum.add("17560232813");
        phoneNum.add("14154522844");
        phoneNum.add("14392458687");
        phoneNum.add("17063744459");
        phoneNum.add("18709447318");
        phoneNum.add("19714859547");
        phoneNum.add("13847776108");
        phoneNum.add("15185056818");
        phoneNum.add("13144648921");
        phoneNum.add("14047846832");
        phoneNum.add("14003952848");
        phoneNum.add("18252989872");
        phoneNum.add("13760602622");
        phoneNum.add("19721415296");
        phoneNum.add("16681236351");
        phoneNum.add("16815508236");
        phoneNum.add("17254260021");
        phoneNum.add("16796980338");
        phoneNum.add("14351188022");
        phoneNum.add("13038384658");
        phoneNum.add("13002379979");
        phoneNum.add("16687602733");
        phoneNum.add("13948870600");
        phoneNum.add("16837661133");
        phoneNum.add("15752670115");
        phoneNum.add("18657343498");
        phoneNum.add("18698777995");
        phoneNum.add("14493972794");
        phoneNum.add("13111837376");
        phoneNum.add("16698164974");
        phoneNum.add("19826588765");
        phoneNum.add("18258311048");
        phoneNum.add("18597719744");
        phoneNum.add("19112753434");
        phoneNum.add("16144545968");
        phoneNum.add("14644492451");
        phoneNum.add("14811659633");
        phoneNum.add("16030482345");
        phoneNum.add("13082473161");
        phoneNum.add("13692850852");
        phoneNum.add("13493005702");
        phoneNum.add("19165498353");
        phoneNum.add("13748863098");
        phoneNum.add("14594696590");
        phoneNum.add("13276976683");
        phoneNum.add("19954319578");
        phoneNum.add("18323588367");
        phoneNum.add("13425016199");

        phoneName.put("14546682403", "周伟业");
        phoneName.put("17567800948", "林子明");
        phoneName.put("15077057946", "庄静丽");
        phoneName.put("14526013645", "陈小婷");
        phoneName.put("14496041839", "张杨平");
        phoneName.put("13084471848", "武冠轩");
        phoneName.put("19429951573", "周泉江");
        phoneName.put("13374117844", "余炜鹏");
        phoneName.put("16691824296", "张灿云");
        phoneName.put("15871463970", "许伟谊");
        phoneName.put("13077212752", "郭锦贤");
        phoneName.put("17560232813", "朱泽亮");
        phoneName.put("14154522844", "吴羽静");
        phoneName.put("14392458687", "张铉");
        phoneName.put("17063744459", "蔡迪全");
        phoneName.put("18709447318", "林炎秀");
        phoneName.put("19714859547", "梁梦怡");
        phoneName.put("13847776108", "刁慧彬");
        phoneName.put("15185056818", "陈增宏");
        phoneName.put("13144648921", "刘瑞雄");
        phoneName.put("14047846832", "刘俊声");
        phoneName.put("14003952848", "陈学斌");
        phoneName.put("18252989872", "王纯淇");
        phoneName.put("13760602622", "梁绍荣");
        phoneName.put("19721415296", "梁成鑫");
        phoneName.put("16681236351", "廖庭康");
        phoneName.put("16815508236", "张华扬");
        phoneName.put("17254260021", "熊欢");
        phoneName.put("16796980338", "马晓华");
        phoneName.put("14351188022", "梁夏天");
        phoneName.put("13038384658", "叶学斌");
        phoneName.put("13002379979", "麦亮森");
        phoneName.put("16687602733", "陈文创");
        phoneName.put("13948870600", "苏日锐");
        phoneName.put("16837661133", "庄海富");
        phoneName.put("15752670115", "黄宇康");
        phoneName.put("18657343498", "张铭");
        phoneName.put("18698777995", "杨晴晴");
        phoneName.put("14493972794", "姚能燕");
        phoneName.put("13111837376", "陈培凤");
        phoneName.put("16698164974", "杜国仲");
        phoneName.put("19826588765", "黄俊轩");
        phoneName.put("18258311048", "邓伟键");
        phoneName.put("18597719744", "叶钦帅");
        phoneName.put("19112753434", "何俊霖");
        phoneName.put("16144545968", "文嘉星");
        phoneName.put("14644492451", "王程锋");
        phoneName.put("14811659633", "邹子庆");
        phoneName.put("16030482345", "邓校君");
        phoneName.put("13082473161", "杨君胜");
        phoneName.put("13692850852", "陈煜");
        phoneName.put("13493005702", "罗锦坚");
        phoneName.put("19165498353", "黄锦晖");
        phoneName.put("13748863098", "吴文悦");
        phoneName.put("14594696590", "李伟豪");
        phoneName.put("13276976683", "李亮蓉");
        phoneName.put("19954319578", "邱泽松");
        phoneName.put("18323588367", "杨建民");
        phoneName.put("13425016199", "杨泳志");
    }

    /**
     * 生产数据
     *
     * @return
     */
    public String productLog() throws ParseException {

        String caller;
        //主叫
        String callee;
        //被叫
        String buildTime;
        //通话时间
        String duration;
        //通话时长

        String callerName;
        String calleeName;

        //1.随机生成两个不同的电话号
        //随机获取主叫手机号
        int callerIndex = (int) (Math.random() * phoneNum.size());
        System.out.println("打印一下主叫手机号" + callerIndex);
        caller = phoneNum.get(callerIndex);
        callerName = phoneName.get(caller);

        //随机获取被叫手机号
        while (true) {
            int calleeIndex = (int) (Math.random() * phoneNum.size());
            callee = phoneNum.get(calleeIndex);
            calleeName = phoneName.get(callee);
            //防止主叫与被叫手机号相同的情况发生
            if (!caller.equals(callee)) {
                break;
            }
        }

        //2.随机生成通话建立时间（start，end）
        buildTime = randBuildTime(start, end);

        //3.随机生成通话时长
        //数据格式化成0000的格式
        DecimalFormat decimalFormat = new DecimalFormat("0000");
        duration = decimalFormat.format((Math.random() * 30 * 60 + 1));

        return caller + "," + callee + "," + buildTime + "," + duration + "\n";
    }

    //随机生成通话时间
    private String randBuildTime(String start, String end) throws ParseException {
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long startPoint = sdf1.parse(start).getTime();
        long endPoint = sdf1.parse(end).getTime();

        long resultTimeString = (long) (startPoint + Math.random() * (endPoint - startPoint));

        return sdf2.format(new Date(resultTimeString));
    }

    //将数据写入到文件中
    public void writeLog(String path) throws IOException, ParseException, InterruptedException {
        FileOutputStream fileOutputStream = new FileOutputStream(path);
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream);

        while (true) {
            String log = productLog();
            System.out.print(log);
            Thread.sleep(500);
            outputStreamWriter.write(log);
            outputStreamWriter.flush();
        }
    }

    public static void main(String[] args) throws InterruptedException, ParseException, IOException {

        if (args.length <= 0) {
            System.out.println("没参数！");
            System.exit(0);
        }
        Producer producer = new Producer();
        producer.intiPhone();
        producer.writeLog(args[0]);
    }

}