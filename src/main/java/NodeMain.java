import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.io.File;

public class NodeMain {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java NodeMain <NodeName>");
            System.exit(1);
        }
        String myName = args[0];

        // 1. Read the JSON configuration file
        ObjectMapper mapper = new ObjectMapper();
        List<DSMNode> allNodes = mapper.readValue(
                new File("dsm_config.json"),
                new TypeReference<List<DSMNode>>() {}
        );

        // 2. Find the DSMNode with this name
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

        // 3. Debugging the correct intialization of the node
        System.out.println("Loaded DSMNode for " + myName
                + ", range " + myNode.getStartAddress() + " .. " + myNode.getEndAddress()
                + ", primary? " + myNode.isPrimary()
                + ", replicas = " + myNode.getReplicaNodes());


    }
}


