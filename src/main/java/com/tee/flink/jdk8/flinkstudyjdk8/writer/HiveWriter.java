package com.tee.flink.jdk8.flinkstudyjdk8.writer;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * @author youchao.wen
 * @date 2023/8/14.
 */
public class HiveWriter extends RichSinkFunction<JSONObject> {
    private transient Statement st = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        Connection con = DriverManager.getConnection("jdbc:hive2://47.243.131.115:10001/demo_schema", "", "");
        st = con.createStatement();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(JSONObject json, Context context) throws Exception {
        Integer id = json.getInteger("id");
        String actor = json.getString("actor");
        String alias = json.getString("alias");
        String sql = "insert into demo_schema.demo(id, actor, alias) VALUES ({id}, '{actor}', '{alias}')"
                .replace("{id}", Integer.toString(id))
                .replace("{actor}", actor)
                .replace("{alias}", alias);

        System.out.println("Running: " + sql);
        st.execute(sql);
    }
}
