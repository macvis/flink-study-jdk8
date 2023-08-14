package com.tee.flink.jdk8.flinkstudyjdk8.connector;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author youchao.wen
 * @date 2023/8/14.
 */
@Slf4j
public class HiveJdbcConnector {

    /**
     * @param args
     * @throws java.sql.SQLException
     */
    public static void main(String[] args) {
        execHiveSql(null, null,
                "INSERT into default.test SELECT * FROM hdfs.text_table",
                "jdbc:hive2://47.97.113.63:10000");

    }

    /**
     * hive执行多个sql
     *
     * @param username
     * @param password
     * @param hiveSql
     * @param hiveJdbcUrl
     * @return
     */
    public static boolean execHiveSql(String username, String password, String hiveSql, String hiveJdbcUrl) {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        try {
            log.info("hiveJdbcUrl:{}", hiveJdbcUrl);
            log.info("username:{}", username);
            log.info("password:{}", password);
            Connection conn = DriverManager.getConnection(hiveJdbcUrl, username, password);
            Statement stmt = conn.createStatement();

            String[] hiveSqls = hiveSql.split(";");
            for (int i = 0; i < hiveSqls.length; i++) {
                if (StringUtils.isNotEmpty(hiveSqls[i])) {
                    stmt.execute(hiveSqls[i]);
                }
            }
            stmt.close();
            conn.close();
            return true;
        } catch (SQLException sqlException) {
            log.error(sqlException.getMessage(), sqlException);
            return false;
        }
    }

}
