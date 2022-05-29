package com.atguigu.bigdata;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * <h4>test22222</h4>
 * <p>描述</p>
 *
 * @author : realdengziqi
 * @date : 2022-05-30 04:02
 **/
public class UnboundedWC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("hadoop102", 9999)
                .flatMap(new FlatMapFunction<String, String>() {

                    public void flatMap(String line,
                                        Collector<String> out) throws Exception {
                        for (String word : line.split(" ")) {
                            out.collect(word);
                        }
                    }
                })
                .map(new MapFunction<String, Tuple2<String, Long>>() {

                    public Tuple2<String, Long> map(String word) throws Exception {
                        return Tuple2.of(word, 1L);
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {

                    public String getKey(Tuple2<String, Long> t) throws Exception {
                        return t.f0;  // t._1
                    }
                })
                .sum(1)
                .print();

        env.execute();

    }

}
