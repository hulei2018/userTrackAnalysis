package com.usertrack.jdbc;

import com.usertrack.conf.ConfigurationManager;
import com.usertrack.constant.Constants;

import java.sql.*;
import java.util.LinkedList;

public class Jdbc_Help {
    //1.加载驱动包
    static {
        String jdbc_driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
        try {
            Class.forName(jdbc_driver);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //2.构建单例对象
    private static Jdbc_Help instants = null;

    public static Jdbc_Help getIntants() {
        synchronized (Jdbc_Help.class) {
            if (instants == null) {
                return new Jdbc_Help();
            }
            return instants;
        }
    }

    //3.利用构造函数进行初始化
    LinkedList<Connection> datasource=new LinkedList<Connection>();
    private Jdbc_Help(){
        int datasourcesize=ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);
        for(int i=0;i<datasourcesize;i++){
            String jdbc_url=ConfigurationManager.getProperty(Constants.JDBC_URL);
            String jdbc_user=ConfigurationManager.getProperty(Constants.JDBC_USER);
            String jdbc_password=ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
            Connection con = null;
            try {
                con = DriverManager.getConnection(jdbc_url,jdbc_user,jdbc_password);
                datasource.push(con);
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
    }

    //4、生成connection连接
    public synchronized  Connection getConnection(){
        if(datasource.size()==0){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }

    //5、归还连接池
    public void returnConnetion(Connection con){
        if(con!=null){
            try {
                if(con.isClosed()){
                    String jdbc_url=ConfigurationManager.getProperty(Constants.JDBC_URL);
                    String jdbc_user=ConfigurationManager.getProperty(Constants.JDBC_USER);
                    String jdbc_password=ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
                    Connection con2 = DriverManager.getConnection(jdbc_url,jdbc_user,jdbc_password);
                    datasource.push(con2);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            datasource.push(con);
        }
    }

    //6.执行查询语句
    public void executionQury(String sql,Object[] params,CallBack callback){
        Connection con = getConnection();
        try {
            PreparedStatement pstm = con.prepareStatement(sql);
            if(params!=null){
                for(int i=0;i<params.length;i++){
                    pstm.setObject(i+1,params[i]);
                }
            }
            ResultSet res = pstm.executeQuery();
            callback.process(res);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 内部今天类提供一个回调ResultSet的接口
     * @para res
     */
    public static interface CallBack{
        void process(ResultSet res) throws Exception;
    }
}
