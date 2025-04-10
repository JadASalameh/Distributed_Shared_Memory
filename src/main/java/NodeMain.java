import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.util.ArrayList;
import java.util.List;

public class NodeMain {
    private static final String REQUEST_QUEUE = "config_request_queue";

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java NodeMain <NodeName>");
            System.exit(1);
        }
        String myName = args[0];
        // Create a temporary reply queue name for the config reply.
        String replyQueue = "config_reply_" + myName;

        // Connect to RabbitMQ.
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // 1. Declare our temporary reply queue.
            channel.queueDeclare(replyQueue, false, false, true, null);

            // 2. Send our reply queue name as the message body to request the config.
            channel.basicPublish("", REQUEST_QUEUE, null, replyQueue.getBytes());
            System.out.println("Requested config, waiting on " + replyQueue);

            // 3. Poll for the config reply.
            GetResponse response = null;
            while (response == null) {
                response = channel.basicGet(replyQueue, true);
                if (response == null) Thread.sleep(100);
            }

            byte[] body = response.getBody();
            ObjectMapper mapper = new ObjectMapper();
            // Deserialize the JSON into a list of DSMNode objects.
            List<DSMNode> allNodes = mapper.readValue(body, new TypeReference<List<DSMNode>>() {});

            // 4. Extract node names from the config to rebuild the PartitionConfig.
            List<String> nodeNames = new ArrayList<>();
            for (DSMNode node : allNodes) {
                nodeNames.add(node.getName());
            }
            if (nodeNames.isEmpty()) {
                System.err.println("No nodes found in configuration.");
                System.exit(1);
            }

            PartitionConfig partitionConfig = new PartitionConfig(1000, 2, nodeNames);




            // Find the DSMNode corresponding to this node.
            DSMNode myNode = null;
            for (DSMNode node : allNodes) {
                if (node.getName().equals(myName)) {
                    myNode = node;
                    break;
                }
            }
            if (myNode == null) {
                System.err.println("No DSMNode found for name = " + myName);
                System.exit(2);
            }

            myNode.setPartitionConfig(partitionConfig);
            myNode.setMessagingService(new RabbitMQMessagingService());

            System.out.println("Loaded DSMNode for " + myName
                    + ", range " + myNode.getStartAddress() + " .. " + myNode.getEndAddress()
                    + ", primary? " + myNode.isPrimary()
                    + ", replicas = " + myNode.getReplicaNodes());

            // 7. Start the node listening for DSM messages.
            myNode.start();

            // Keep the node running.
            while (true) {
                Thread.sleep(1000);
            }
        }
    }
}





