import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class NodeMain {
    private static final String REQUEST_QUEUE = "config_request_queue";
    // Original node order from ConfigServer (MUST match ConfigServer's nodeNames)
    private static final List<String> ORIGINAL_NODE_ORDER = List.of(
            "NodeA", "NodeA1", "NodeA2", "NodeB", "NodeB1", "NodeB2"
    );

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java NodeMain <NodeName>");
            System.exit(1);
        }
        String myName = args[0];
        String replyQueue = "config_reply_" + myName;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(replyQueue, false, false, true, null);
            channel.basicPublish("", REQUEST_QUEUE, null, replyQueue.getBytes());
            System.out.println("Requested config, waiting on " + replyQueue);

            GetResponse response = null;
            while (response == null) {
                response = channel.basicGet(replyQueue, true);
                if (response == null) Thread.sleep(100);
            }

            byte[] body = response.getBody();
            ObjectMapper mapper = new ObjectMapper();
            List<DSMNode> allNodes = mapper.readValue(body, new TypeReference<List<DSMNode>>() {});

            // Sort the nodes according to the original ConfigServer order
            allNodes.sort(Comparator.comparing(node -> ORIGINAL_NODE_ORDER.indexOf(node.getName())));

            // Extract node names in the original order
            List<String> nodeNames = new ArrayList<>();
            for (DSMNode node : allNodes) {
                nodeNames.add(node.getName());
            }

            if (nodeNames.isEmpty()) {
                System.err.println("No nodes found in configuration.");
                System.exit(1);
            }

            PartitionConfig partitionConfig = new PartitionConfig(1000, 2, nodeNames);

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

            myNode.start();

            while (true) {
                Thread.sleep(1000);
            }
        }
    }
}




