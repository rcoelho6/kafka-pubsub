package myapps;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class Producer {
    private static String topico = "streams-all-messages";

    private static List<String> mensages = Arrays.asList(
            "{\"type\":\"adr\",\"content\":{\"person\":\"Antonio\",\"street\":\"Rua Lavandisca\",\"number\":\"10\",\"neighborhood\":\"Moema\",\"city\":\"São Paulo\",\"state\":\"SP\",\"country\":\"\",\"zipcode\":\"01301010\"}}",
            "{\"type\":\"adr\",\"content\":{\"person\":\"Marcos\",\"street\":\"Av Ibirapuera\",\"number\":\"1658\",\"neighborhood\":\"Moema\",\"city\":\"São Paulo\",\"state\":\"SP\",\"country\":\"\",\"zipcode\":\"04352010\"}}",
            "{\"type\":\"adr\",\"content\":{\"person\":\"44d1cb3f-57e1-4e6e-81df-b30181ae054e\",\"street\":\"Av 23 de maio\",\"number\":\"23456\",\"neighborhood\":\"Moema\",\"city\":\"São Paulo\",\"state\":\"SP\",\"country\":\"\",\"zipcode\":\"01506020\"}}",
            "{\"type\":\"adr\",\"content\":{\"person\":\"f51d3127-08c2-45a6-8797-212bedce5945\",\"street\":\"Rua Sabiá\",\"number\":\"15\",\"neighborhood\":\"Moema\",\"city\":\"São Paulo\",\"state\":\"SP\",\"country\":\"\",\"zipcode\":\"01708910\"}}",
            "{\"type\":\"adr\",\"content\":{\"person\":\"16681882-a3c2-4315-a9e8-cfddb5ffeff5\",\"street\":\"Rua Roxinol\",\"number\":\"123\",\"neighborhood\":\"Moema\",\"city\":\"São Paulo\",\"state\":\"SP\",\"country\":\"\",\"zipcode\":\"04337018\"}}",
            "{\"type\":\"adr\",\"content\":{\"person\":\"37882e13-7e23-4b36-b85f-ed0f4d735c71\",\"street\":\"Rua Tagua\",\"number\":\"111\",\"neighborhood\":\"Liberdade\",\"city\":\"São Paulo\",\"state\":\"SP\",\"country\":\"\",\"zipcode\":\"01509011\"}}",
            "{\"type\":\"adr\",\"content\":{\"person\":\"f81d3127-08c2-45a6-8797-212bedce5945\",\"street\":\"Rua Euclides Pacheco\",\"number\":\"15\",\"neighborhood\":\"Tatuapé\",\"city\":\"São Paulo\",\"state\":\"SP\",\"country\":\"\",\"zipcode\":\"03907040\"}}",
            "{\"type\":\"adr\",\"content\":{\"person\":\"19681882-a3c2-4315-a9e8-cfddb5ffeff5\",\"street\":\"Rua Galvão Bueno\",\"number\":\"123\",\"neighborhood\":\"Liberdade\",\"city\":\"São Paulo\",\"state\":\"SP\",\"country\":\"\",\"zipcode\":\"04587000\"}}",
            "{\"type\":\"adr\",\"content\":{\"person\":\"30882e13-7e23-4b36-b85f-ed0f4d735c71\",\"street\":\"Rua da Gloria\",\"number\":\"10\",\"neighborhood\":\"Liberdade\",\"city\":\"São Paulo\",\"state\":\"SP\",\"country\":\"\",\"zipcode\":\"01900110\"}}",
            "{\"type\":\"adr\",\"content\":{\"person\":\"32167084-85a0-4fb5-865b-bb2aae2e1581\",\"street\":\"Av Liberdade\",\"number\":\"1658\",\"neighborhood\":\"Liberdade\",\"city\":\"São Paulo\",\"state\":\"SP\",\"country\":\"\",\"zipcode\":\"04321099\"}}",
            "{\"type\":\"adr\",\"content\":{\"person\":\"42d1cb3f-57e1-4e6e-81df-b30181ae054e\",\"street\":\"Av  Vergueiro\",\"number\":\"23456\",\"neighborhood\":\"Liberdade\",\"city\":\"São Paulo\",\"state\":\"SP\",\"country\":\"\",\"zipcode\":\"03893001\"}}",
            "{\"type\":\"adr\",\"content\":{\"person\":\"f11d3127-08c2-45a6-8797-212bedce5945\",\"street\":\"Rua Serra de Jureia\",\"number\":\"15\",\"neighborhood\":\"Tatuapé\",\"city\":\"São Paulo\",\"state\":\"SP\",\"country\":\"\",\"zipcode\":\"5671423\"}}",
            "{\"type\":\"adr\",\"content\":{\"person\":\"12681882-a3c2-4315-a9e8-cfddb5ffeff5\",\"street\":\"Rua Canta Galo\",\"number\":\"123\",\"neighborhood\":\"Tatuapé\",\"city\":\"São Paulo\",\"state\":\"SP\",\"country\":\"\",\"zipcode\":\"1357982\"}}",
            "{\"type\":\"adr\",\"content\":{\"person\":\"33167084-85a0-4fb5-865b-bb2aae2e1581\",\"street\":\"Av Salin Sara Maluf\",\"number\":\"1658\",\"neighborhood\":\"Tatuapé\",\"city\":\"São Paulo\",\"state\":\"SP\",\"country\":\"\",\"zipcode\":\"0112240\"}}",
            "{\"type\":\"adr\",\"content\":{\"person\":\"44d1cb3f-57e1-4e6e-81df-b30181ae054e\",\"street\":\"Av Alcantara Machado\",\"number\":\"23456\",\"neighborhood\":\"Tatuapé\",\"city\":\"São Paulo\",\"state\":\"SP\",\"country\":\"\",\"zipcode\":\"01818900\"}}",
            "{\"type\":\"adr\",\"content\":{\"person\":\"15681882-a3c2-4315-a9e8-cfddb5ffeff5\",\"street\":\"Rua Azevedo Soaresl\",\"number\":\"123\",\"neighborhood\":\"Tatuapé\",\"city\":\"São Paulo\",\"state\":\"SP\",\"country\":\"\",\"zipcode\":\"017899010\"}}"
    );

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        // IP e Porta do broker bootstrap
        properties.put("bootstrap.servers", "localhost:9092");

        // Id do producer que esta se connectando no broker
        properties.put("client.id", "cliente.1");

        // A chave será serializada para Long
        properties.put("key.serializer", LongSerializer.class.getName());

        // O valor será serializado para String
        properties.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        while (true) {
            Integer random = (new Random()).nextInt(mensages.size()-1);
            String mensage = mensages.get(random);

            System.out.println("Send on topic " + topico + " the message: " + mensage);
            ProducerRecord<String,String> record = new ProducerRecord<>(topico, mensage);
            producer.send(record);
            Thread.sleep(5000);
        }
    }
}
