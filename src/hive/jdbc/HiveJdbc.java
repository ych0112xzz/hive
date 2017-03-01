package hive.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.log4j.Logger;



public class HiveJdbc {
    public static void main(String[] args) throws  Exception {
        Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
        
       // String dropSQL="drop table javabloger";
        //String createSQL="create table javabloger (key int, value string)";
       //hive��������֧�����ַ�ʽһ�֣�load�ļ�����һ��Ϊ����һ�����в�ѯ���в��루�о����Ǹ����ߣ� 
       //hive�ǲ�֧��insert into...values(....)���ֲ�����
        //String insterSQL="LOAD DATA LOCAL INPATH '/work/hive/examples/files/kv1.txt' OVERWRITE INTO TABLE javabloger";
        String querySQL="SELECT a.* FROM company a";
        
        Connection con = DriverManager.getConnection("jdbc:hive2://datanode4:10000/default", "hive", "");
        Statement stmt = con.createStatement();
        //stmt.executeQuery(dropSQL);  // ִ��ɾ�����
        //stmt.executeQuery(createSQL);  // ִ�н������
        //stmt.executeQuery(insterSQL);  // ִ�в������
        ResultSet res = stmt.executeQuery(querySQL);   // ִ�в�ѯ���
        
          while (res.next()) {
            System.out.println("Result: key:"+res.getString(1) +"  �C>  value:" +res.getString(2));
        }
    }
}