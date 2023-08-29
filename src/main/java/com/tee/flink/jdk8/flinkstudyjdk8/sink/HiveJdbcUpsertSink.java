package com.tee.flink.jdk8.flinkstudyjdk8.sink;

import com.alibaba.fastjson.JSON;
import com.tee.flink.jdk8.flinkstudyjdk8.entity.Demo;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author youchao.wen
 * @date 2023/8/29.
 */
@Slf4j
public class HiveJdbcUpsertSink extends RichSinkFunction<Demo> {
    private transient Statement st = null;

    private String tableName;


    private CompletableFuture<Void> completionFuture;

    public HiveJdbcUpsertSink tableName(String tableName){
        this.tableName = tableName;
        return this;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection con = DriverManager.getConnection("jdbc:hive2://47.243.131.115:10001/demo_schema", "", "");
        st = con.createStatement();

        completionFuture = new CompletableFuture<>();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (completionFuture != null && !completionFuture.isDone()) {
            completionFuture.complete(null);
        }
    }

    @Override
    public void invoke(Demo cdcData, Context context) throws Exception {
        log.info("cdcData = {}", JSON.toJSONString(cdcData));
        if(this.existed(cdcData.getId(), st)){
            this.doUpdate(cdcData, st);
        }else{
            this.doInsert(cdcData, st);
        }

        st.close();

        // 每次写入完成后，将 completionFuture 标记为已完成
        if (completionFuture != null && !completionFuture.isDone()) {
            completionFuture.complete(null);
        }
    }

    private boolean existed(int id, Statement st) throws Exception{
        // 检查数据是否存在
        String query = "select * from demo_schema." + this.tableName + " where id=" + id;
        ResultSet rs = st.executeQuery(query);
        if(rs != null && rs.next()){
            log.info("id={}的数据已存在", id);
            return true;
        }

        return false;
    }

    private void doInsert(Demo data, Statement st) throws Exception{
        Integer id = data.getId();

        String actor = data.getActor();
        String alias = data.getAlias();
        String insert = "insert into demo_schema." + this.tableName + "(id, actor, alias) VALUES ({id}, '{actor}', '{alias}')"
                .replace("{id}", Integer.toString(id))
                .replace("{actor}", actor)
                .replace("{alias}", alias);

        log.info("hive insert SQL = {}", insert);
        st.execute(insert);
    }


    private synchronized void doDelete(Demo data, Statement st) {
        Integer id = data.getId();
        String delete = "delete from demo_schema." + this.tableName + " where id={id}"
                .replace("{id}", Integer.toString(id));
        try{
            log.info("hive delete SQL = {}", delete);
            st.execute(delete);
        }catch(Exception e){
            log.error("delete error", e);
        }
    }

    private synchronized void doUpdate(Demo data, Statement st){
        Integer id = data.getId();
        String update = "update demo_schema." + this.tableName
                + " set actor={actor}, alias={alias}"
                + " where id={id}"
                .replace("{id}", Integer.toString(id))
                .replace("{actor}", data.getActor())
                .replace("{alias}", data.getAlias());
        try{
            log.info("hive delete SQL = {}", update);
            st.execute(update);
        }catch(Exception e){
            log.error("delete error", e);
        }
    }

    // 在预提交阶段等待所有写入任务完成
    public void preCommit(long checkpointId) throws ExecutionException, InterruptedException {
        if (completionFuture != null) {
            completionFuture.get(); // 等待所有写入任务完成
        }
    }
}
