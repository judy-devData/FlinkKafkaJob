package org.example;
import com.mongodb.client.model.InsertOneModel;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.bson.BsonDocument;
import org.apache.flink.connector.base.DeliveryGuarantee;



import java.time.Instant;


public class FlinkKafkaProcess  {
    public static void main(String[] args) {
        try {
            // Set up the execution environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            KafkaSource<Object> source = KafkaSource.<Object>builder()
                    .setBootstrapServers("kafka:9092")
                    .setTopics("DataChange.dev.users")
                    .setGroupId("flink-consumer")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new CustomSchema())
                    .build();




            DataStream<Object> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");


            // Apply windowing and count the insertions
            DataStream<Tuple2<String, Integer>> result = stream
                    .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                    .process(new CountInsertionsProcessWindowFunction());

            // Sink the result to MongoDB
            MongoSink<Tuple2<String, Integer>> sink = MongoSink.<Tuple2<String, Integer>>builder()
                    .setUri("mongodb://dev_data:Abcdevpoc@172.17.0.1:27017")
                    .setDatabase("my_db")
                    .setCollection("DataAggregated")
                    .setBatchSize(1)
                    .setMaxRetries(3)
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .setSerializationSchema((input, context) ->
                            new InsertOneModel<>(BsonDocument.parse("{\"date\": \"" + Instant.now() + "\", \"count\": " + input.f1 + "}")))
                    .build();

            result.sinkTo(sink);

            env.execute("From Kafka to MongoDB");

        } catch (Exception e) {
            // Handle the exception
            e.printStackTrace();
        }
    }

    // ProcessWindowFunction to count the number of insertions in each window
    public static class CountInsertionsProcessWindowFunction extends ProcessAllWindowFunction<Object, Tuple2<String, Integer>, TimeWindow> {

        @Override
        public void process(Context context, Iterable<Object> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
            int count = 0;
            for (Object element : elements) {
                ObjectMapper mapper = new ObjectMapper();
                String jsonString = mapper.writeValueAsString(element);
                System.out.println("Input: " + jsonString); // Print the input as JSON string

                // Check if the element represents an insertion
                if (isInsertion(jsonString)) {
                    count++;
                }
            }
            out.collect(new Tuple2<>("insertions", count));
        }


        // Check if the JSON string represents an insertion from thr op field in the data change capture
        private boolean isInsertion(String jsonString) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readTree(jsonString);
                JsonNode opNode = jsonNode.get("op");
                if (opNode != null && opNode.asText().equals("c")) {
                    return true;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return false;
        }
    }
}