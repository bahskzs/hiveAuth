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
    private static final String URLHIVE = "jdbc:hive2://192.168.41.241:10000/default";
    private static Connection connection = null;

    public static Connection getHiveConnection() {
        if (null == connection) {
            synchronized (HiveJDBC.class) {
                if (null == connection) {
                    try {
                        Class.forName("org.apache.hive.jdbc.HiveDriver");
                        connection = DriverManager.getConnection(URLHIVE, "apexinfo", "Apexinfo@2020!!");
                        System.out.println("hive启动连接成功！");
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

        String sql1="select * from ods_pms_bpm_company_t_dss_psm_corpdishones_orc limit 1";
        PreparedStatement pstm = getHiveConnection().prepareStatement(sql1);
        ResultSet rs= pstm.executeQuery(sql1);

        while (rs.next()) {
            System.out.println(rs.getString(2));
        }
        pstm.close();
        rs.close();

    }

}
