import java.sql.*;

public class JDBCExample_ldap {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException
    {
        try
        {
            Class.forName(driverName);
        } catch (ClassNotFoundException e)
        {
            e.printStackTrace();
            System.exit(1);
        }
        // Hive2 JDBC URL with LDAP
        String jdbcURL = "jdbc:hive2://172.16.140.76:10000/tpch_orc_2";
        // 访问HiveServer2的用户的用户名
        String user = "hive";
        // 访问HiveServer2的用户的密码
        String password = "123456";
        Connection conn = DriverManager.getConnection(jdbcURL, user, password);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select c_name,c_nationkey from customer limit 10;");
        while (rs.next())
        {
            System.out.print(rs.getString(1) + "   ");
            System.out.println(rs.getString(2));
        }
        rs.close();
        stmt.close();
        conn.close();
    }

}
