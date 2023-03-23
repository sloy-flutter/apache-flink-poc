package com.flutter.kafka.producer;

import com.flutter.pojos.Bet;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaSourceProducer {
    private final static String SOURCE_TOPIC = "poc-source";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static List<String> RUNNERS = Arrays.asList(
//            "Mr Muldon",
//            "Alnadam",
//            "Better Getalong",
//            "Vocal Duke",
//            "Hasty Brook",
//            "On To Victory",
//            "Hurlerontheditch",
//            "Malpas",
            "Faron",
            "Applaus"
    );

    public static void main(String[] args) {
        runKafkaSourceProducer(5);
    }

    private static void runKafkaSourceProducer(final int messageCount) {
        final Producer<Long, String> producer = createProducer();

        try{
            for (long i = 0L; i < messageCount; i++){

                final ProducerRecord<Long, String> record =
                        new ProducerRecord<>(SOURCE_TOPIC, i, generateBet());

                producer.send(record).get();

                Thread.sleep(getRandomSleepTime());
            }
        }
        catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            producer.flush();
            producer.close();
        }
    }

    private static Producer<Long, String> createProducer(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaSourceProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    private static String generateBet(){
        return new Bet(getRandomBetSelection(), getRandomBetLiability()).toString();
    }

    private static String getRandomBetSelection(){
        return RUNNERS.get(generateRandomNumber(RUNNERS.size()-1, 0));
    }
    private static int getRandomBetLiability(){
        int maxLiability = 10;
        int minLiability = 0;
        return generateRandomNumber(maxLiability, minLiability);
    }
    private static int getRandomSleepTime(){
        int maxSleep = 3000;
        int minSleep = 0;
        return generateRandomNumber(maxSleep, minSleep);
    }

    private static int generateRandomNumber(int max, int min){
        Random r = new Random();
        return r.nextInt((max-min)+1);
    }
}
