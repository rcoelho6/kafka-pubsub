package myapps;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Consumer {
    private static List<String> topics_type_1 = Arrays.asList("streams-all-messages", "streams-only-addresses","streams-updated-addresses");
    private static List<String> topics_type_2 = Arrays.asList("streams-count-updated-addresses");

    public static void main(String[] args) {
        Properties properties = new Properties();

        // IP e Porta do broker bootstrap
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // Id do grupo do tópico que esta se conectando no broker
        properties.setProperty("group.id", "group.1");

        // Id do producer que esta se conectando no broker
        properties.setProperty("client.id", "cliente.1");

        // Define se o commit das mensagens lidas vai ser automático
        properties.setProperty("enable.auto.commit", "true");

        // Tempo maximo de espera do commit até ser considerado falha
        properties.setProperty("auto.commit.interval.ms", "5000");

        // Pega o offset mais recente ao se conectar com o kafka
        properties.setProperty("auto.offset.reset", "earliest");

        // Máximo de mensagem que irá ser pega por vez (lote)
        properties.setProperty("max.poll.records", "500");

        // Tempo do heartbeat que o componente irá enviar ao kafka
        properties.setProperty("heartbeat.interval.ms", "1000");

        // Espera do heartbeat do componente até ser considerado falha
        properties.setProperty("session.timeout.ms", "10000");

        KafkaConsumer<String, String> consumerType1 = getConsumer(properties, topics_type_1, StringDeserializer.class.getName());
        KafkaConsumer<String, Long> consumerType2 = getConsumer(properties, topics_type_2, LongDeserializer.class.getName());

        try {
            while (true) {

                consumerType1.poll(Duration.ofSeconds(10)).forEach(record -> {
                        System.out.println(record.topic() + ": " + record.key() + " - " + record.value());
                });

                consumerType2.poll(Duration.ofSeconds(10)).forEach(record -> {
                    System.out.println(record.topic() + ": " + record.key() + " - " + record.value());
                });

            }
        } finally {
            consumerType1.close();
            consumerType2.close();
        }
    }

    private static <T> KafkaConsumer<String, T> getConsumer(Properties properties, List<String> topics_type, String valueDeserializer) {
        // A chave será serializada para Long
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());

        // O valor será serializado para String
        properties.setProperty("value.deserializer", valueDeserializer);

        KafkaConsumer<String, T> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(topics_type);
        return consumer;
    }
}
