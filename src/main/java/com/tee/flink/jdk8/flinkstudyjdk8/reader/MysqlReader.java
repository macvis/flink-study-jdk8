package com.tee.flink.jdk8.flinkstudyjdk8.reader;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author youchao.wen
 * @date 2023/8/14.
 */
public class MysqlReader extends RichSourceFunction<JSONObject> {
    private transient Statement st = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/flinkdemo?useCursorFetch=true", "root", "1234567890");
        st = con.createStatement();
        st.setFetchSize(3);
    }



    @Override
    public void run(SourceContext<JSONObject> ctx) throws Exception {

        ResultSet rs = st.executeQuery("select * from flinkdemo.demo");

        while (rs.next()) {
            Integer id = rs.getInt("id");
            String actor = rs.getString("actor");
            String alias = rs.getString("alias");

            JSONObject json = new JSONObject();
            json.put("id", id);
            json.put("actor", actor);
            json.put("alias", alias);
            ctx.collect(json);
        }

        //rs.close();
        //st.close();
        //con.close();
    }

    @Override
    public void cancel() {

    }

}
