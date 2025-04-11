import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.rabbitmq.client.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Client {
    private static final String CONFIG_REQUEST_QUEUE = "config_request_queue";
    private static final Map<Integer, Long> addressSequenceNumbers = new HashMap<>();
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java Client <request_file>");
            System.exit(1);
        }

        List<String> lines = Files.readAllLines(Paths.get(args[0]));

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // ********** REQUEST CONFIGURATION FROM SERVER **********
            String configReplyQueue = "client_config_reply_" + UUID.randomUUID();
            channel.queueDeclare(configReplyQueue, false, false, true, null);
            System.out.println("Client: Requested config, waiting on " + configReplyQueue);

            channel.basicPublish("", CONFIG_REQUEST_QUEUE, null, configReplyQueue.getBytes(StandardCharsets.UTF_8));

            GetResponse configResponse = null;
            while (configResponse == null) {
                configResponse = channel.basicGet(configReplyQueue, true);
                if (configResponse == null) Thread.sleep(100);
            }

            byte[] configBody = configResponse.getBody();
            ObjectMapper mapper = new ObjectMapper();
            List<DSMNode> nodes = mapper.readValue(configBody, new TypeReference<List<DSMNode>>() {});
            List<String> nodeNames = new ArrayList<>();
            for (DSMNode node : nodes) nodeNames.add(node.getName());

            if (nodeNames.isEmpty()) {
                System.err.println("No nodes found in configuration.");
                return;
            }

            Random random = new Random();
            ObjectMapper objectMapper = new ObjectMapper();
            String targetNode = nodeNames.get(random.nextInt(nodeNames.size()));

            System.out.println("Client is processing requests from file: " + args[0]);

            for (String line : lines) {
                line = line.trim();
                if (line.isEmpty()) continue;

                String[] tokens = line.split("\\s+");
                if (tokens.length < 2) {
                    System.err.println("Invalid command: " + line);
                    continue;
                }

                String operation = tokens[0].toLowerCase();
                int addressValue;
                try {
                    addressValue = Integer.parseInt(tokens[1]);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid address: " + tokens[1]);
                    continue;
                }

                Address address = new Address(addressValue);


                if (operation.equals("read")) {
                    String replyQueue = "client_reply_" + UUID.randomUUID();
                    channel.queueDeclare(replyQueue, false, false, false, null);
                    BlockingQueue<String> replyQueueBlocking = new ArrayBlockingQueue<>(1);

                    channel.basicConsume(replyQueue, true, (consumerTag, delivery) -> {
                        String reply = new String(delivery.getBody(), StandardCharsets.UTF_8);
                        replyQueueBlocking.add(reply);
                    }, consumerTag -> {});
                    long seq = addressSequenceNumbers.getOrDefault(addressValue, 0L);
                    DSMMessage msg = new DSMMessage(DSMMessage.Type.READ, address, null, replyQueue,seq);
                    channel.basicPublish("", targetNode, null, objectMapper.writeValueAsBytes(msg));
                    System.out.println("Sent READ to " + targetNode + " at address " + addressValue);

                    String receivedValue = replyQueueBlocking.take();
                    System.out.println("Value at address " + addressValue + ": " + receivedValue);

                } else if (operation.equals("write")) {
                    if (tokens.length < 3) {
                        System.err.println("Write requires value: " + line);
                        continue;
                    }

                    String value = tokens[2];

                    String writeReplyQueue = "client_write_reply_" + UUID.randomUUID();
                    channel.queueDeclare(writeReplyQueue, false, false, false, null);

                    BlockingQueue<String> ackQueue = new ArrayBlockingQueue<>(1);


                    channel.basicConsume(writeReplyQueue, true, (consumerTag, delivery) -> {
                        String ack = new String(delivery.getBody(), StandardCharsets.UTF_8);
                        ackQueue.add(ack);
                    }, consumerTag -> {});

                    long seq = addressSequenceNumbers.getOrDefault(addressValue, 0L) + 1;
                    DSMMessage msg = new DSMMessage(DSMMessage.Type.WRITE, address, value, writeReplyQueue, seq);
                    channel.basicPublish("", targetNode, null, objectMapper.writeValueAsBytes(msg));
                    System.out.println("Sent WRITE to " + targetNode + ": address " + addressValue + ", value " + value);

                    String ack = ackQueue.take();
                    addressSequenceNumbers.put(addressValue, seq);
                    System.out.println("WRITE confirmed for address " + addressValue + ", ack: " + ack);

                } else {
                    System.err.println("Unknown operation: " + operation);
                }
            }

            System.out.println("Client finished processing all requests.");
        }
    }
}


