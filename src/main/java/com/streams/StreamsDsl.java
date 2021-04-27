package com.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

/**
 * 스트림즈 DSL 구현 클래스
 */
public class StreamsDsl {
    private static String APPLICATION_NAME = "streams-filter-application"; // 애플리케이션 아이디로 사용할 애플리케이션 이름
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092"; // 카프카 서버 정보
    private static String TOPIC_USER_SCORE = "user_score"; // 카피해올 데이터가 있는 토픽명
    private static String TOPIC_PASS_USER_SCORE = "pass_user_score"; // 카피한 데이터를 저장할 토픽명

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        /**
         * 메시지의 키와 값에 직렬화/역직렬화 방식 설정.
         * 스트림즈 애플리케이션에서는 데이터를 처리할 때, 메시지의 키 또는 값을 역직렬화 하고
         * 데이터를 최종 토픽에 전달할 때, 직렬화해서 데이터를 저장한다.
         */
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 스트림 토폴로지를 정의하기 위한 StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();

        /*
        - 소스 프로세서 동작
        user_score 토픽으로부터 KStream 객체를 만든다.
         */
        KStream<String, String> userScore = builder.stream(TOPIC_USER_SCORE);

        /*
        - 스트림 프로세서 동작
        user_score 토픽에서 가져온 데이터 중 value가 10을 넘는 경우의 값만 남도록 필터링하여 KStream 객체를 새롭게 생성
         */
        KStream<String, String> filteredStream = userScore.filter(
                (key, value) -> Integer.parseInt(value) > 10
        );

        /*
        - 싱크 프로세서 동작
        pass_user_score 토픽으로 KStream 데이터를 전달한다.
         */
        filteredStream.to(TOPIC_PASS_USER_SCORE);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}