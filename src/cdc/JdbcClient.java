package cdc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class JdbcClient {

    public static void main(String[] args)
        throws SQLException, ClassNotFoundException
    {
        
        Class.forName("cdc.jdbc.CDCDriver");

        Connection conn = DriverManager.getConnection("jdbc:mysql-cdc:/var/log/mysql/binlog-files.index", new Properties());
        try {
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery("select * from foo.auto");
            ResultSetMetaData rsmeta = rs.getMetaData();
            int columnCount = rsmeta.getColumnCount();
            int rownum = 0;
            
            while (rs.next()) {
                rownum++;
                System.out.print(rownum);
                System.out.print(": ");
                for (int i=1; i<=columnCount; i++) {
                    System.out.print(rs.getObject(i));
                    System.out.print(" ");
                }
                System.out.println();
            }
        }
        finally {
            conn.close();
        }
    }
}
