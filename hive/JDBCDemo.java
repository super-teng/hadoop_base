package com.baiduwaimai.hive;

import org.apache.hive.com.esotericsoftware.kryo.util.Util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by Super腾 on 2016/12/15.
 */
public class JDBCDemo {
    public static void main(String[] args){
        String sql = "select * from t1";
        PreparedStatement ps = null;
        Connection conn = null;
        ResultSet rs = null;
        //通过工具类获得连接来获得预处理状态参数
        try {
            conn = Utils.getConnection();
            ps = conn.prepareStatement(sql);
            //通过预处理参数获得结果集
            rs = ps.executeQuery();
            //处理结果集中的数据
            while(rs.next()){
                int id = rs.getInt(1);
                String name = rs.getString(2);
                String sex =rs.getString(3);
                System.out.println(id+"  "+name+"  "+sex+"  ");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            Utils.release(conn,ps,rs);
        }

    }
}
