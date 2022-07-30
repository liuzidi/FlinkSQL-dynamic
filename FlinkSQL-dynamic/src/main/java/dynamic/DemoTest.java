package dynamic;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.client.deployment.executors.PipelineExecutorUtils;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.net.MalformedURLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class DemoTest {
    static String DDL = "CREATE TABLE table1 (" +
            "  `ID` STRING" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = 'topic01'," +
            "  'properties.bootstrap.servers' = 'localhost:9092'," +
            "  'properties.group.id' = 'testGroup'," +
            "  'scan.startup.mode' = 'latest-offset'," +
            "  'format' = 'csv'" +
            ")" ;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        tabEnv.executeSql(DDL);
        //5s后变更规则
        execute(env, tabEnv, new FindChangeEntity() {
            final long firstTime = System.currentTimeMillis();
            @Override
            public boolean isChanged() {
                return System.currentTimeMillis() - firstTime > 5000;
            }

            @Override
            public StreamGraph returnStreamGraph() {
                String newsql = "select * from table1 where ID < 10";
                return getSQLGraph(newsql);
            }
        },2000L);
    }

    public static void execute(StreamExecutionEnvironment env, StreamTableEnvironment tabEnv, FindChangeEntity fc, long period) {
        String sql = "select * from table1 where ID > 10";
        StreamGraph sqlGraph = getSQLGraph(sql);
//        System.out.println(sqlGraph.getStreamingPlanAsJSON());
//        JobGraph jobGraph = Stream2JobGraph(sqlGraph, env);
        JobClient jobClient = null;
        try {
            env.execute(sqlGraph, fc, period);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static StreamGraph getSQLGraph(String sql) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        tabEnv.executeSql(DDL);
        Table table = tabEnv.sqlQuery(sql);
        tabEnv.toAppendStream(table, Row.class).print();
        return env.getStreamGraph();
    }

    public static JobGraph Stream2JobGraph(StreamGraph sg, StreamExecutionEnvironment env) throws MalformedURLException {
        return PipelineExecutorUtils.getJobGraph(sg, env.getConfiguration());
    }
}
