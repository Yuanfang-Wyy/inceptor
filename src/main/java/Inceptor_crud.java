/**
 *
 */
import org.apache.log4j.*;
import java.sql.*;


public class Inceptor_crud {
    private static Logger logger = Logger.getLogger(Inceptor_crud.class);
    private static String drivername = "org.apache.hive.jdbc.HiveDriver";
    private String url;
    static {
        try {
            Class.forName(drivername);
        }catch (ClassNotFoundException e){
            e.printStackTrace();
            System.exit(1);
        }
    }

    public Inceptor_crud(String inceptorServerHosts){
        this.url = "jdbc:hive2://" + inceptorServerHosts + ":10000/default";
        System.out.println(this.url);
    }

    /**
     * 建表
     */
    void createTable(){
        Connection coon = null;
        Statement st = null;
        try {
            coon = DriverManager.getConnection(url,"hive","123456");
            st = coon.createStatement();
            String sql1 = "drop table if exists wyy.acidTable;";
            String sql2 = "drop table if exists wyy.partitionAcidTable";
            String sql3 = "drop table if exists wyy.range_int_table";
            String sql4 = "create table wyy.acidTable(name string,age int, degree int) clustered by(age) into 10 buckets stored as orc tblproperties(\"transactional\"=\"true\");";
            String sql5 = "create table wyy.partitionAcidTable(name string,age int) partitioned by (city string) clustered by (age) into 10 buckets stored as orc tblproperties(\"transactional\"=\"true\")";
            String sql6 = "create table wyy.range_int_table(id int, value int) partitioned by range (id) (partition less1 values less than (1), partition less10 values less than (10) ) clustered by (value) into 3 buckets stored as orc TBLProperties(\"transactional\"=\"true\")";
            st.execute(sql1);
            st.execute(sql2);
            st.execute(sql3);
            st.execute(sql4);
            st.execute(sql5);
            st.execute(sql6);

        }catch (SQLException e){
            e.printStackTrace();
        }finally {
            closeConnectAndStatement(coon,st);
        }
    }

    /**
     * 关闭连接
     * @param conn
     * @param st
     */
    private void closeConnectAndStatement(Connection conn, Statement st){
        if (null != conn){
            try {
                conn.close();
            }catch (SQLException e){
                e.printStackTrace();
            }
        }
        if (null != st){
            try {
                st.close();
            }catch (SQLException e){
                e.printStackTrace();
            }
        }
    }

    void insert(){
        String sql1 = "set transaction.type=inceptor";
        String sql2 = "Insert into wyy.acidTable(name, age,degree) values ('aaa', 12,23)";
        String sql3 = "Insert into wyy.partitionAcidTable partition(city='sh') (name, age)values ('aaa', 12)";
        Connection conn = null;
        Statement st = null;
        try {
            conn = DriverManager.getConnection(url,"hive","123456");
            st = conn.createStatement();
            st.execute(sql1);
            st.execute(sql2);
            st.execute(sql3);
        }catch (SQLException e){
            e.printStackTrace();
        }finally {
            closeConnectAndStatement(conn,st);
        }
    }

    void batchinsert(){
        String sql1 = "set transaction.type=inceptor";
        String sql2 = "batchinsert into wyy.acidtable(name,age,degree) batchvalues(values('aaa',12,3),values('bbb',22,9))";
        String sql3 = "batchinsert into  wyy.partitionAcidTable partition(city='sh') (name, age) batchvalues(values ('aaa', 12), values ('bbb', 22))";
        Connection conn = null;
        Statement st = null;
        try {
            conn = DriverManager.getConnection(url,"hive","123456");
            st = conn.createStatement();
            st.execute(sql1);
            st.execute(sql2);
            st.execute(sql3);
        }catch (SQLException e){
            e.printStackTrace();
        }finally {
            closeConnectAndStatement(conn,st);
        }
    }

    private void select(){
        String sql = "select * from wyy.acidtable;";
        Connection conn = null;
        Statement st = null;
        try {
            conn = DriverManager.getConnection(url,"hive","123456");
            st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            long i = 1;
            while (rs.next()){
                String name = rs.getString("name");
                Integer degree = rs.getInt("degree");
                System.out.println(name+"----------"+i+"----------"+degree);
                i++;
            }
        }catch (SQLException e){
            e.printStackTrace();
        }finally {
            closeConnectAndStatement(conn,st);
        }
    }


    public static void main(String[] args) throws SQLException{
        Inceptor_crud mytable = new Inceptor_crud("172.16.140.76");
        //mytable.createTable();
        //mytable.insert();
        //mytable.batchinsert();
        mytable.select();
    }

}
