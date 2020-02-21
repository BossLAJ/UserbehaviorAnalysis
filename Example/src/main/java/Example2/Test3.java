package Example2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Test3 {

    private static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.64.154");
        conf.set("hbase.zookeeper.property.clientPort","2181");
    }

    public static void main(String[] args) throws IOException {
//        createTable("dept","deptinfo","childdeptinfo");
        //saveDate("dept");
//       // selectAll("dept");
//        selectTwoAll("dept");
//        selectWebAll("dept");
      //  saveDateDes("dept");
        selectDesignAndDev("dept");
       // saveDeves("dept");
    }

    //建立连接
    public static Connection getCon() throws IOException {
        return ConnectionFactory.createConnection(conf);
    }
    //表
    public static Admin getAdmin() throws IOException {
        return getCon().getAdmin();
    }
    //关闭
    public static void CloseCon(Connection conn) throws IOException {
        conn.close();
    }

    //判断表是否存在
    public static boolean tableExists(String tableName) throws IOException {
        return getCon().getAdmin().tableExists(TableName.valueOf(tableName));
    }
    //创建表
    public static void createTable(String tableName,String...columnFamiles) throws IOException {
        if (tableExists(tableName))return ;
        Admin admin = getAdmin();
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for(String columnFamily:columnFamiles){
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
            hTableDescriptor.addFamily(columnDescriptor);
        }
        admin.createTable(hTableDescriptor);
    }

    //添加数据
    public static void saveDate(String tableName) throws IOException {
        Connection con = getCon();
        HTable table = (HTable) con.getTable(TableName.valueOf(tableName));
        List<Put> puts = new ArrayList<>();

        Put put1 = new Put(Bytes.toBytes("1_01"));
        put1.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("dname"),Bytes.toBytes("ceo"));
        put1.addColumn(Bytes.toBytes("childdeptinfo"),Bytes.toBytes("2_02"),Bytes.toBytes("develop"));
        put1.addColumn(Bytes.toBytes("childdeptinfo"),Bytes.toBytes("2_05"),Bytes.toBytes("web"));
        puts.add(put1);

        Put put2 = new Put(Bytes.toBytes("2_02"));
        put2.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("dname"),Bytes.toBytes("develop"));
        put2.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("super"),Bytes.toBytes("1_01"));
        put2.addColumn(Bytes.toBytes("childdeptinfo"),Bytes.toBytes("3_03"),Bytes.toBytes("develop1"));
        put2.addColumn(Bytes.toBytes("childdeptinfo"),Bytes.toBytes("3_04"),Bytes.toBytes("develop2"));
        put2.addColumn(Bytes.toBytes("childdeptinfo"),Bytes.toBytes("3_08"),Bytes.toBytes("design"));
        puts.add(put2);

        Put put3 = new Put(Bytes.toBytes("2_05"));
        put3.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("dname"),Bytes.toBytes("web"));
        put3.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("super"),Bytes.toBytes("1_01"));
        put3.addColumn(Bytes.toBytes("childdeptinfo"),Bytes.toBytes("3_06"),Bytes.toBytes("web1"));
        put3.addColumn(Bytes.toBytes("childdeptinfo"),Bytes.toBytes("3_07"),Bytes.toBytes("web2"));
        puts.add(put3);

        Put put4 = new Put(Bytes.toBytes("3_03"));
        put4.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("dname"),Bytes.toBytes("develop1"));
        put4.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("super"),Bytes.toBytes("2_02"));
        puts.add(put4);

        Put put5 = new Put(Bytes.toBytes("3_04"));
        put5.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("dname"),Bytes.toBytes("develop2"));
        put5.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("super"),Bytes.toBytes("2_02"));
        puts.add(put5);

        Put put6 = new Put(Bytes.toBytes("3_06"));
        put6.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("dname"),Bytes.toBytes("web1"));
        put6.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("super"),Bytes.toBytes("2_05"));
        puts.add(put6);

        Put put7 = new Put(Bytes.toBytes("3_07"));
        put7.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("dname"),Bytes.toBytes("web2"));
        put7.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("super"),Bytes.toBytes("2_05"));
        puts.add(put7);

        table.put(puts);
        CloseCon(con);
    }

    //查询所有数据
    public static void selectAll(String tableName) throws IOException {
        Connection conn = getCon();
        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for(Result result : scanner ){
            //部门编号
            String s = Bytes.toString(result.getRow());
            String s1 = s.split("_")[1];
            System.out.println("部门编号:"+s1);

            //部门名称
            byte[] value = result.getValue(Bytes.toBytes("deptinfo"), Bytes.toBytes("dname"));
            System.out.println("部门名称:"+Bytes.toString(value));

            //上级部门
            byte[] value1 = result.getValue(Bytes.toBytes("deptinfo"), Bytes.toBytes("super"));
            if(value1 == null ){
                System.out.println("上级部门:"+"无");
            }else {
                Get get = new Get(value1);
                Result result1 = table.get(get);
                byte[] value2 = result1.getValue(Bytes.toBytes("deptinfo"), Bytes.toBytes("dname"));
                System.out.println("上级部门:"+Bytes.toString(value2));
            }
            //下级部门
            Scan scan1 = new Scan();
            ResultScanner scanner1 = table.getScanner(scan);
            String string = "";
            for (Result result1 : scanner1){
                byte[] childdeptinfos = result.getValue(Bytes.toBytes("childdeptinfo"), result1.getRow());
                if(childdeptinfos != null){
                    string+=","+Bytes.toString(childdeptinfos);
                }
            }
            if(string.length() > 0){
                System.out.println("下属部门:"+string.substring(1));
            }else if(string.length() == 0){
                System.out.println("下属部门:"+"无");
            }
            System.out.println("------------------------");
        }
    }

    //查询所有二级部门
    public static void selectTwoAll(String tableName) throws IOException {
        Connection con = getCon();
        HTable table = (HTable) con.getTable(TableName.valueOf(tableName));

        PrefixFilter filter = new PrefixFilter(Bytes.toBytes("2_"));
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner){
            //部门编号
            String row1 = Bytes.toString(result.getRow());
            String row2 = row1.split("_")[1];
            System.out.println("部门编号:"+row2);

            //部门名称
            byte[] departname = result.getValue(Bytes.toBytes("deptinfo"), Bytes.toBytes("dname"));
            System.out.println("部门名称:"+Bytes.toString(departname));

            //上级部门
            byte[] superName = result.getValue(Bytes.toBytes("deptinfo"), Bytes.toBytes("super"));
            if(superName == null){
                System.out.println("上级部门:"+"无");
            }else {
                Get get = new Get(superName);
                Result result1 = table.get(get);
                byte[] value = result1.getValue(Bytes.toBytes("deptinfo"), Bytes.toBytes("dname"));
                System.out.println("上级部门:"+Bytes.toString(value));
            }
            //下属部门
            Scan scan1 = new Scan();
            ResultScanner scanner1 = table.getScanner(scan);
            String string = "";
            for (Result result1 : scanner1){
                byte[] childdepartinfos = result.getValue(Bytes.toBytes("childdeptinfo"), result1.getRow());
                if(childdepartinfos !=null){
                    string +=","+Bytes.toString(childdepartinfos);
                }
            }
            if(string.length()>0){
                System.out.println("下属部门:"+string.substring(1));
            }else{
                System.out.println("下属部门:"+"无");
            }
            System.out.println("-----------------------------------");
        }
    }

    //查询web机器所属部门的部门编号，部门名称，和下部门信息
    public static void selectWebAll(String tableName) throws IOException {
        Connection con = getCon();
        HTable table = (HTable) con.getTable(TableName.valueOf(tableName));
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes("deptinfo"),
                Bytes.toBytes("dname"),
                CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("web")));
        Scan scan = new Scan();
        scan.setFilter(singleColumnValueFilter);
        ResultScanner scanner = table.getScanner(scan);
        for(Result result : scanner){
            //部门编号
            String s = Bytes.toString(result.getRow());
            String s1 = s.split("_")[1];
            System.out.println("部门编号:"+s1);

            //部门名称
            byte[] value = result.getValue(Bytes.toBytes("deptinfo"), Bytes.toBytes("dname"));
            System.out.println("部门名称:"+Bytes.toString(value));

            //上级部门
            byte[] value1 = result.getValue(Bytes.toBytes("deptinfo"), Bytes.toBytes("super"));
            if(value1 == null){
                System.out.println("上级部门:"+"无");
            }else{
                Get get = new Get(value1);
                Result result1 = table.get(get);
                byte[] value2 = result1.getValue(Bytes.toBytes("deptinfo"), Bytes.toBytes("dname"));
                System.out.println("上级部门:"+Bytes.toString(value2));
            }
            //下属部门
            Scan scan2 = new Scan();
            ResultScanner scanner1 = table.getScanner(scan2);
            String string = "";
            for (Result result1 : scanner1){
                byte[] childdeptinfos = result.getValue(Bytes.toBytes("childdeptinfo"), result1.getRow());
                if(childdeptinfos != null){
                    string +=","+Bytes.toString(childdeptinfos);
                }
            }
            if(string.length() > 0 ){
                System.out.println("下级部门:"+string.substring(1));
            }else {
                System.out.println("下级部门:"+ "无");
            }
            System.out.println("--------------------------------------------");
        }
    }
    //新增部门design,该部门直属ceo
    //其下有两个部门分别是app-ui和web-ui
    public static void saveDateDes(String tableName) throws IOException {
        Connection con = getCon();
        HTable table = (HTable) con.getTable(TableName.valueOf(tableName));
        List<Put> puts = new ArrayList<>();
        Put put = new Put(Bytes.toBytes("2_03"));
        put.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("dname"),Bytes.toBytes("design"));
        put.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("super"),Bytes.toBytes("1_01"));
        put.addColumn(Bytes.toBytes("childdeptinfo"),Bytes.toBytes("3_08"),Bytes.toBytes("app-ui"));
        put.addColumn(Bytes.toBytes("childdeptinfo"),Bytes.toBytes("3_09"),Bytes.toBytes("web-ui"));
        puts.add(put);

        Put put1 = new Put(Bytes.toBytes("3_08"));
        put1.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("dname"),Bytes.toBytes("app-ui"));
        put1.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("super"),Bytes.toBytes("2_03"));
        puts.add(put1);

        Put put2 = new Put(Bytes.toBytes("3_09"));
        put2.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("dname"),Bytes.toBytes("web-ui"));
        put2.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("super"),Bytes.toBytes("app-ui"));
        puts.add(put2);

        Put put3 = new Put(Bytes.toBytes("1_01"));
        put3.addColumn(Bytes.toBytes("childdeptinfo"),Bytes.toBytes("2_03"),Bytes.toBytes("design"));
        puts.add(put3);
        table.put(puts);
    }
    //将design调整为develop部门的下属
    public static void saveDeves(String tableName) throws IOException {
        Connection con = getCon();
        HTable table = (HTable) con.getTable(TableName.valueOf(tableName));
        List<Put> list = new ArrayList<Put>();

        Put put3 = new Put(Bytes.toBytes("2_02"));
        put3.addColumn(Bytes.toBytes("childdeptinfo"), Bytes.toBytes("3_08"), Bytes.toBytes("design"));

        Put put = new Put(Bytes.toBytes("3_08"));
        put.addColumn(Bytes.toBytes("deptinfo"), Bytes.toBytes("dname"), Bytes.toBytes("design"));
        put.addColumn(Bytes.toBytes("deptinfo"), Bytes.toBytes("super"), Bytes.toBytes("2_02"));
        put.addColumn(Bytes.toBytes("childdeptinfo"), Bytes.toBytes("4_09"), Bytes.toBytes("app-ui"));
        put.addColumn(Bytes.toBytes("childdeptinfo"), Bytes.toBytes("4_10"), Bytes.toBytes("web-ui"));

        Put put2 = new Put(Bytes.toBytes("4_09"));
        put2.addColumn(Bytes.toBytes("deptinfo"), Bytes.toBytes("dname"), Bytes.toBytes("app-ui"));
        put2.addColumn(Bytes.toBytes("deptinfo"), Bytes.toBytes("super"), Bytes.toBytes("3_08"));

        Put put1 = new Put(Bytes.toBytes("4_10"));
        put1.addColumn(Bytes.toBytes("deptinfo"), Bytes.toBytes("dname"), Bytes.toBytes("web-ui"));
        put1.addColumn(Bytes.toBytes("deptinfo"), Bytes.toBytes("super"), Bytes.toBytes("3_08"));



        list.add(put);
        list.add(put1);
        list.add(put2);
        list.add(put3);

        table.put(list);
    }

    //查询design和develop
    public static void selectDesignAndDev(String tableName) throws IOException {
        Connection con = getCon();
        HTable table = (HTable) con.getTable(TableName.valueOf(tableName));
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("deptinfo"), Bytes.toBytes("dname"),
                CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("develop")));
        SingleColumnValueFilter singleColumnValueFilter1 = new SingleColumnValueFilter(Bytes.toBytes("deptinfo"), Bytes.toBytes("dname"),
                CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("design")));
        Scan scan = new Scan();
        scan.setFilter(singleColumnValueFilter);
        Scan scan1 = new Scan();
        scan1.setFilter(singleColumnValueFilter1);
        ResultScanner scanner = table.getScanner(scan);
        ResultScanner scanner1 = table.getScanner(scan1);
        for (Result result : scanner){
            //部门编号
            String s = Bytes.toString(result.getRow());
            String s1 = s.split("_")[1];
            System.out.println("部门编号:"+s1);

            //部门名称
            byte[] value = result.getValue(Bytes.toBytes("deptinfo"), Bytes.toBytes("dname"));
            System.out.println("部门名称:"+Bytes.toString(value));
            //上级部门
            byte[] value1 = result.getValue(Bytes.toBytes("deptinfo"), Bytes.toBytes("super"));
            if(value == null ){
                System.out.println("上级部门:"+"无");
            }else {
                Get get = new Get(value1);
                Result result1 = table.get(get);
                byte[] value2 = result1.getValue(Bytes.toBytes("deptinfo"), Bytes.toBytes("dname"));
                System.out.println("上级部门:"+Bytes.toString(value2));
            }
            //下级部门
            Scan scan2 = new Scan();
            ResultScanner scanner2 = table.getScanner(scan2);
            String string = "";
            for (Result result1 : scanner2){
                byte[] childdeptinfos = result.getValue(Bytes.toBytes("childdeptinfo"), result1.getRow());
                if(childdeptinfos != null ){
                    string +=","+Bytes.toString(childdeptinfos);
                }
            }
            if(string.length() > 0){
                System.out.println("下级部门:"+string.substring(1));
            }else{
                System.out.println("下级部门:"+"无");
            }
            System.out.println("--------------------------");
        }

        for (Result result : scanner1){
            //部门编号
            String s2 = Bytes.toString(result.getRow());
            String s3 = s2.split("_")[1];
            System.out.println("部门编号:"+s3);

            //部门名称
            byte[] value = result.getValue(Bytes.toBytes("deptinfo"), Bytes.toBytes("dname"));
            System.out.println("部门名称:"+Bytes.toString(value));

            //上级部门
            byte[] value1 = result.getValue(Bytes.toBytes("deptinfo"), Bytes.toBytes("super"));
            if(value1 == null){
                System.out.println("上级部门:"+"无");
            }else{
                Get get = new Get(value1);
                Result result1 = table.get(get);
                byte[] value2 = result1.getValue(Bytes.toBytes("deptinfo"), Bytes.toBytes("dname"));
                System.out.println("上级部门:"+Bytes.toString(value2));
            }

            //下级部门
            Scan scan2 = new Scan();
            ResultScanner scanner2 = table.getScanner(scan);
            String string = "";
            for (Result result1 : scanner2){
                byte[] childdeptinfos = result.getValue(Bytes.toBytes("childdeptinfo"), result1.getRow());
                if(childdeptinfos != null ){
                    string +=","+Bytes.toString(childdeptinfos);
                }
            }
            if(string.length() > 0){
                System.out.println("下级部门:"+string.substring(1));
            }else{
                System.out.println("下级部门:"+"无");
            }
            System.out.println("--------------------------");
        }
    }
}
