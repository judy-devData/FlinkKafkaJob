# FlinkKafkaJob

streaming process in Java that aggregates the events in Kafka by windowing the events on each 10 seconds, and count only the number of insertion of new items (only the new items, not the deleted or the updated), and store them in Mongo in a collection containing the date and the result.
