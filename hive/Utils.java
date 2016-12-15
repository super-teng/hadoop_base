package com.baiduwaimai.hive;

import java.sql.*;

/**
 * Created by Super腾 on 2016/12/15.
 */
//JDBC连接hive时的工具类
public class Utils{
    //驱动类的全限定名
    private static final String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
    //连接hive的URL路径
    private static final String url = "jdbc:hive://192.168.1.111:10000/default";
    static{
        //加载驱动
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    public static Connection getConnection() {
        Connection conn = null;
        //获取连接
        try {
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            return conn;
        }
    }
    //释放资源
    public static void release(Connection conn, Statement stat, ResultSet rs){
        try {
            if(conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            if(stat != null) {
                stat.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            if(rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
