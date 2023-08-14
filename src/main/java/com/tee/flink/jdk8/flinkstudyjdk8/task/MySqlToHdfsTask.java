package com.tee.flink.jdk8.flinkstudyjdk8.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.hive.shaded.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Component;

/**
 * @author youchao.wen
 * @date 2023/8/9.
 */
@Slf4j
@Component
@Deprecated
public class MySqlToHdfsTask {

    public static void main(String[] args) {
        new MySqlToHdfsTask().trigger();
    }

    public void trigger(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername("com.mysql.cj.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/flinkdemo")
                .setUsername("root")
                .setPassword("1234567890")
                .setQuery("select * from demo")
                .setRowTypeInfo(new RowTypeInfo(
                        BasicTypeInfo.LONG_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO))
                .finish();

        String userSchema = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"demo\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"id\", \"type\": \"long\"},\n" +
                "    {\"name\": \"actor\", \"type\": \"string\"},\n" +
                "    {\"name\": \"alias\", \"type\": \"string\"}\n" +
                "  ]\n" +
                "}";

        Schema schema = new Schema.Parser().parse(userSchema);

        DataStream<GenericRecord> dataStream = env.createInput(jdbcInputFormat)
//                .map((MapFunction<Row, GenericRecord>) value -> {
                .map(value -> {
                    // Convert row to Avro generic record
                    // You will need a suitable avro schema matching your Hive table structure
                    // This part is omitted due to complexity of conversion. Implement based on your requirements.
                    GenericData.Record record = new GenericData.Record(schema);
                    record.put("id", value.getField(0));
                    record.put("actor", value.getField(1));
                    record.put("alias", value.getField(2));
                    return record;
                });


        FileSink<GenericRecord> sink = FileSink
                .forBulkFormat(new Path("hdfs://localhost:9000/hive"),
                        ParquetAvroWriters.forGenericRecord(schema))
                .build();


        dataStream.sinkTo(sink);

        try{
            env.execute("MySQL to HDFS Flink Job");
        }catch(Exception e){
            log.error("", e);
        }
    }
}
