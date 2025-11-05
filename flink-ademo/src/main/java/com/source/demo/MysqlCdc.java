package com.source.demo;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlCdc {
    public static void main(String[] args) throws Exception {
        // Flink CDC 3.x 配置
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("9.134.75.242")
                .port(3306)
                .databaseList("flink") // 监控的数据库列表
                .tableList("flink.User") // 监控的表列表，格式：database.table
                .username("root")
                .password("asdfQWER123")
                .serverId("5400-5500") // 必需：MySQL server id 范围
                .serverTimeZone("UTC") // 时区配置，必须与 MySQL 服务器时区一致
                .deserializer(new JsonDebeziumDeserializationSchema()) // 必需：反序列化器
                .startupOptions(StartupOptions.initial()) // 使用 initial 模式，先读取快照再读取增量
                .includeSchemaChanges(false) // 是否包含 schema 变更
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000 * 30); // 启用 checkpoint，1分钟一次

        // 从 MySQL CDC source 读取数据
        DataStream<String> cdcStream = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MySQL CDC Source"
        ).setParallelism(1);

        // 转换 CDC 数据为 UserRecord
        DataStream<UserRecord> userStream = cdcStream
                .map(new CdcRecordParser())
                .filter(record -> record != null) // 过滤掉 null 记录
                .name("Parse CDC Records");

        // 使用 JDBC Connector 写入 User2 表
        userStream.addSink(
                JdbcSink.sink(
                        // SQL 语句：使用 REPLACE INTO 或 INSERT ... ON DUPLICATE KEY UPDATE
                        "REPLACE INTO User2 (id, name, age, date) VALUES (?, ?, ?, ?)",

                        // PreparedStatement 参数设置
                        new JdbcStatementBuilder<UserRecord>() {
                            @Override
                            public void accept(PreparedStatement ps, UserRecord record) throws SQLException {
                                ps.setInt(1, record.id);
                                ps.setString(2, record.name);
                                ps.setObject(3, record.age); // 使用 setObject 处理可能的 null
                                ps.setDate(4, record.date);
                            }
                        },

                        // JDBC 执行选项
                        JdbcExecutionOptions.builder()
                                .withBatchSize(100)              // 批次大小
                                .withBatchIntervalMs(1000)       // 批次间隔 1 秒
                                .withMaxRetries(3)               // 最大重试次数
                                .build(),

                        // JDBC 连接选项
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://9.134.75.242:3306/flink?useSSL=false&serverTimezone=UTC")
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("asdfQWER123")
                                .build()
                )
        ).name("JDBC Sink to User2");

        env.execute("MySQL CDC User to User2 Job");
    }

    /**
     * 用户记录 POJO
     */
    public static class UserRecord {
        public Integer id;
        public String name;
        public Integer age;
        public Date date;
        public String operation; // c=create, u=update, d=delete, r=read

        public UserRecord() {}

        public UserRecord(Integer id, String name, Integer age, Date date, String operation) {
            this.id = id;
            this.name = name;
            this.age = age;
            this.date = date;
            this.operation = operation;
        }

        @Override
        public String toString() {
            return "UserRecord{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", age=" + age +
                    ", date=" + date +
                    ", operation='" + operation + '\'' +
                    '}';
        }
    }

    /**
     * CDC 数据解析器
     */
    public static class CdcRecordParser implements MapFunction<String, UserRecord> {
        @Override
        public UserRecord map(String value) throws Exception {
            try {
                JSONObject record = JSON.parseObject(value);
                String op = record.getString("op");

                if (op == null) {
                    System.out.println("Unknown operation, skipping: " + value);
                    return null;
                }

                // 对于 DELETE 操作，我们不需要写入 User2（REPLACE INTO 会自动处理）
                // 如果需要处理删除，可以使用单独的流
                if ("d".equals(op)) {
                    System.out.println("Delete operation detected, skipping: " + record.getJSONObject("before"));
                    return null;
                }

                // 获取数据（INSERT/UPDATE 使用 after，READ 也使用 after）
                JSONObject data = record.getJSONObject("after");
                if (data == null) {
                    return null;
                }

                // 解析字段
                Integer id = data.getInteger("id");
                String name = data.getString("name");
                Integer age = data.getInteger("age");

                // 解析日期字段
                Date date = null;
                Object dateObj = data.get("date");
                if (dateObj != null) {
                    if (dateObj instanceof Long) {
                        date = new Date((Long) dateObj);
                    } else if (dateObj instanceof Integer) {
                        // Debezium 可能将日期表示为自 epoch 以来的天数
                        date = new Date(((Integer) dateObj).longValue() * 86400000L);
                    } else {
                        // 尝试解析字符串格式的日期
                        String dateStr = dateObj.toString();
                        date = Date.valueOf(dateStr);
                    }
                }

                UserRecord userRecord = new UserRecord(id, name, age, date, op);
                System.out.println("Parsed record: " + userRecord);

                return userRecord;

            } catch (Exception e) {
                System.err.println("Error parsing CDC record: " + value);
                e.printStackTrace();
                return null;
            }
        }
    }
}
