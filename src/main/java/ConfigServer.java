import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class ConfigServer {
    private static final String REQUEST_QUEUE = "config_request_queue";

    public static void main(String[] args) throws Exception {
        // 1. Create the config
        int totalAddresses = 1000;
        int replicationFactor = 2;
        List<String> nodeNames = List.of("NodeA", "NodeA1", "NodeA2",
                "NodeB", "NodeB1", "NodeB2");

        PartitionConfig partitionConfig = new PartitionConfig(totalAddresses, replicationFactor, nodeNames);
        List<DSMNode> dsmNodes = DSMNodeFactory.createNodesFrom(partitionConfig);

        ObjectMapper mapper = new ObjectMapper();
        byte[] configData = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(dsmNodes);

        // -- Write the config data to disk for debugging --
        Path debugFile = Path.of("config_debug.json");
        Files.write(debugFile, configData);
        System.out.println("Debug: wrote JSON config to " + debugFile.toAbsolutePath());

        // 2. Set up RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(REQUEST_QUEUE, false, false, false, null);
            System.out.println("ConfigServer is waiting for config requests...");

            DeliverCallback callback = (consumerTag, delivery) -> {
                // Get reply queue name from message body
                String replyQueue = new String(delivery.getBody());

                // Send config to that queue
                channel.queueDeclare(replyQueue, false, false, true, null);
                channel.basicPublish("", replyQueue, null, configData);
                System.out.println("Sent config to " + replyQueue);
            };

            channel.basicConsume(REQUEST_QUEUE, true, callback, consumerTag -> {});
            while (true) {
                Thread.sleep(1000); // keep the server alive
            }
        }
    }
}


