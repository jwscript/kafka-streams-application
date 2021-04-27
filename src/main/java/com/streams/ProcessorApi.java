package com.streams;

import com.filter.ProcessorFilter;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class ProcessorApi {
    private static String APPLICATION_NAME = "processor-application"; // 애플리케이션 아이디로 사용할 애플리케이션 이름
    private static String BOOTSTRAP_SERVERS = "54.180.119.14:9092"; // 카프카 서버 정보
    private static String TOPIC_USER_SCORE = "user_score"; // 카피해올 데이터가 있는 토픽명
    private static String TOPIC_PASS_USER_SCORE2 = "pass_user_scoøre2"; // 카피한 데이터를 저장할 토픽명

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        /*
        메시지의 키와 값에 직렬화/역직렬화 방식 설정.
        스트림즈 애플리케이션에서는 데이터를 처리할 때, 메시지의 키 또는 값을 역직렬화 하고
        데이터를 최종 토픽에 전달할 때, 직렬화해서 데이터를 저장한다.
         */
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology topology = new Topology();
        topology.addSource("Source", TOPIC_USER_SCORE)
                .addProcessor("Process", () -> new ProcessorFilter(), "Source")
                .addSink("Sink", TOPIC_PASS_USER_SCORE2, "Process");


        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
    }
}