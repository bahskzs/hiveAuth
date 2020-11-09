package com.hive;/**
 * @author cat
 * @date 2020-09-18 17:00
 */

import java.sql.*;
import org.apache.hive.jdbc.HiveDriver;
/**
 * HiveJDBC
 * @author cat
 * @date 2020-09-18 17:00
 */
public class HiveJDBC {
    private static final String URLHIVE = "jdbc:hive2://10.96.12.230:10005/default;";
    private static Connection connection = null;

    public static Connection getHiveConnection() {
        if (null == connection) {
            synchronized (HiveJDBC.class) {
                if (null == connection) {
                    try {
                        Class.forName("org.apache.hive.jdbc.HiveDriver");
                        connection = DriverManager.getConnection(URLHIVE, "apexinfo", "Apexinfo@2020!!");

                    } catch (SQLException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return connection;
    }


    public static void main(String args[]) throws SQLException{

        Connection con = DriverManager.getConnection("jdbc:hive2://10.96.12.230:10005/default", "hive", "Apexinfo@2020!!");
        Statement stmt = con.createStatement();
        String tableName = "test4";
        stmt.execute("create table "+ tableName +" (key int, value string)");
        String sql ="select count(*) from ods_pms_bpm_company_t_dss_psm_corpdishones_orc ";
        ResultSet res = stmt.executeQuery(sql);
        System.out.println("开始获取记录~~~~");
        if (res.next()) {
            System.out.println(res.getString(1));
        }
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }
        stmt.close();
        res.close();

    }

}
