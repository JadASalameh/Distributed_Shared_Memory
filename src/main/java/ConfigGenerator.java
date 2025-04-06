import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.List;

public class ConfigGenerator {
    public static void main(String[] args) throws Exception {
        // 1. We define what the overall configuration of the DSM
        int totalAddresses = 1000;
        int replicationFactor = 1;
        List<String> nodeNames = List.of("NodeA", "NodeB", "NodeC", "NodeD");


        // 2. Build the PartitionConfig
        PartitionConfig partitionConfig = new PartitionConfig(totalAddresses, replicationFactor, nodeNames);
        RabbitMQMessagingService dummyMessaging = new RabbitMQMessagingService();

        // 3. Generate the DSMNodes using the factory
        List<DSMNode> dsmNodes = DSMNodeFactory.createNodesFrom(partitionConfig,dummyMessaging);

        // 4. Serialize them to JSON (using Jackson) and write to a file
        ObjectMapper mapper = new ObjectMapper();
        mapper.writerWithDefaultPrettyPrinter()
                .writeValue(new File("dsm_config.json"), dsmNodes);

        System.out.println("Configuration file 'dsm_config.json' generated successfully!");
    }
}
