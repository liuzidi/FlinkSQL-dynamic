package dynamic;

import org.apache.flink.client.deployment.executors.PipelineExecutorUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DemoTest {
    public static void main(String[] args) throws Exception {
        String DDL = "CREATE TABLE table1 (" +
                "  `ID` STRING" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'topic01'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'properties.group.id' = 'testGroup'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'csv'" +
                ")" ;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        tabEnv.executeSql(DDL);
        String sql = "select * from table1 where ID > 2";
        StreamGraph sqlGraph = getSQLGraph(sql, DDL);
        System.out.println(sqlGraph.getStreamingPlanAsJSON());
//        PipelineExecutorUtils.getJobGraph(sqlGraph, env.getConfig());
        env.executeAsync(sqlGraph);

    }

    public static StreamGraph getSQLGraph(String sql, String DDL) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        tabEnv.executeSql(DDL);
        Table table = tabEnv.sqlQuery(sql);
        tabEnv.toAppendStream(table, Row.class).print();
        return env.getStreamGraph();
    }
}
