package Example_Month;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Test4 {
    private static Configuration conf;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.64.154");
        conf.set("hbase.zookeeper.property.clientPort","2181");
    }

    //主方法
    public static void main(String[] args) throws IOException {
        //createTable("dept1","deptinfo","childdeptinfo");
        saveDate("dept1");
       //selectAll("dept1");
        //selectWeb("dept1");
    }

    //创建连接
    public static Connection getCon() throws IOException {
        return ConnectionFactory.createConnection(conf);
    }

    //表连接
    public static Admin getAdmin() throws IOException {
        return getCon().getAdmin();
    }
    //关闭连接
    public static void closeConnection(Connection conn) throws IOException {
        conn.close();
    }

    //判断表是否存在
    public static boolean tableExists(String tableName) throws IOException {
        return getCon().getAdmin().tableExists(TableName.valueOf(tableName));
    }

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
        Connection conn = getCon();
        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        List<Put> puts = new ArrayList<Put>();
        Put put = new Put(Bytes.toBytes("1_01"));
        put.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("dname"),Bytes.toBytes("ceo"));
        put.addColumn(Bytes.toBytes("childdeptinfo"),Bytes.toBytes("2_02"),Bytes.toBytes("develop"));
        put.addColumn(Bytes.toBytes("childdeptinfo"),Bytes.toBytes("2_05"),Bytes.toBytes("web"));
        puts.add(put);

        Put put1 = new Put(Bytes.toBytes("2_02"));
        put1.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("dname"),Bytes.toBytes("develop"));
        put1.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("super"),Bytes.toBytes("1_01"));
        put1.addColumn(Bytes.toBytes("childdeptinfo"),Bytes.toBytes("3_03"),Bytes.toBytes("develop1"));
        put1.addColumn(Bytes.toBytes("childdeptinfo"),Bytes.toBytes("3_04"),Bytes.toBytes("develop2"));
        puts.add(put1);

        Put put2 = new Put(Bytes.toBytes("2_05"));
        put2.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("dname"),Bytes.toBytes("web"));
        put2.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("super"),Bytes.toBytes("1_01"));
        put2.addColumn(Bytes.toBytes("childdeptinfo"),Bytes.toBytes("3_06"),Bytes.toBytes("web1"));
        put2.addColumn(Bytes.toBytes("childdeptinfo"),Bytes.toBytes("3_07"),Bytes.toBytes("web2"));
        puts.add(put2);

        Put put3 = new Put(Bytes.toBytes("3_03"));
        put3.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("dname"),Bytes.toBytes("develop1"));
        put3.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("super"),Bytes.toBytes("2_02"));
        puts.add(put3);

        Put put4 = new Put(Bytes.toBytes("3_04"));
        put3.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("dname"),Bytes.toBytes("develop2"));
        put3.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("super"),Bytes.toBytes("2_02"));
        puts.add(put4);

        Put put5 = new Put(Bytes.toBytes("3_06"));
        put5.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("dname"),Bytes.toBytes("web1"));
        put5.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("super"),Bytes.toBytes("2_05"));
        puts.add(put5);

        Put put6 = new Put(Bytes.toBytes("3_07"));
        put6.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("dname"),Bytes.toBytes("web2"));
        put6.addColumn(Bytes.toBytes("deptinfo"),Bytes.toBytes("super"),Bytes.toBytes("2_05"));
        puts.add(put6);

        table.put(puts);
        closeConnection(conn);
    }

    public static void selectAll(String tableName) throws IOException {
        //获得连接
        Connection conn = getCon();
        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
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
            if(value1 == null ){
                System.out.println("上级部门:"+"无");
            }else{
                Get get = new Get(value1);
                Result result1 = table.get(get);
                byte[] value2 = result1.getValue(Bytes.toBytes("deptinfo"), Bytes.toBytes("dname"));
                System.out.println("上级部门:"+Bytes.toString(value2));
            }
            //下级部门
            Scan scan1 = new Scan();
            ResultScanner scanner1 = table.getScanner(scan1);
            String string = "";
            for(Result result1 : scanner1){
                byte[] value2 = result.getValue(Bytes.toBytes("childdeptinfo"), result1.getRow());
                if(value2 != null){
                    string += ","+Bytes.toString(value2);
                }
            }
            if(string.length() > 0){
                System.out.println("下级部门:"+string.substring(1));
            }else{
                System.out.println("下级部门:"+"无");
            }
            System.out.println("-------------------------");
        }
    }

    //查询所有二级部门的部门编号、部门名称和下属部门的信息
    public static void selectWeb(String tableName) throws IOException {
        //获得连接
        Connection con = getCon();
        HTable table = (HTable) con.getTable(TableName.valueOf(tableName));
        PrefixFilter filter = new PrefixFilter(Bytes.toBytes("2_"));
        Scan scan = new Scan();
        Scan scan1 = scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan1);
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
            if(value1 == null ){
                System.out.println("上级部门:"+"无");
            }else{
                Get get = new Get(value1);
                Result result1 = table.get(get);
                byte[] value2 = result1.getValue(Bytes.toBytes("deptinfo"), Bytes.toBytes("dname"));
                System.out.println("上级部门:"+Bytes.toString(value2));
            }

            //下级部门
            Scan scan2= new Scan();
            ResultScanner scanner1 = table.getScanner(scan2);
            String string = "";
            for(Result result1 : scanner1){
                byte[] value2 = result.getValue(Bytes.toBytes("childdeptinfo"), result1.getRow());
                if(value2 != null){
                    string += ","+Bytes.toString(value2);
                }
            }
            if(string.length() >0){
                System.out.println("下级部门:"+string.substring(1));
            }else{
                System.out.println("下级部门:"+"无");
            }
            System.out.println("------------------------");
        }
    }
}
