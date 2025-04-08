import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.util.List;

public class ConfigServer {
    private static final String REQUEST_QUEUE = "config_request_queue";

    public static void main(String[] args) throws Exception {
        // 1. Create the config
        int totalAddresses = 1000;
        int replicationFactor = 0;
        List<String> nodeNames = List.of("NodeA", "NodeB", "NodeC", "NodeD");

        PartitionConfig partitionConfig = new PartitionConfig(totalAddresses, replicationFactor, nodeNames);
        List<DSMNode> dsmNodes = DSMNodeFactory.createNodesFrom(partitionConfig);

        ObjectMapper mapper = new ObjectMapper();
        byte[] configData = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(dsmNodes);

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
            while (true) Thread.sleep(1000); // keep alive
        }
    }
}

