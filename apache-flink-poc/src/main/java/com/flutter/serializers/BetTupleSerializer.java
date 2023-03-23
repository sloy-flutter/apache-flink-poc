package com.flutter.serializers;

import com.flutter.pojos.Bet;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;

public class BetTupleSerializer implements SerializationSchema<Tuple2<String, Integer>> {
    @Override
    public byte[] serialize(Tuple2<String, Integer> betTuple) {
        return betTuple.toString().getBytes();
    }
}
