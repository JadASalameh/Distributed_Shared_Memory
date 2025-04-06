import java.util.*;

public class DSMNodeFactory {
    public static List<DSMNode> createNodesFrom(PartitionConfig config,MessagingService messaging) {
        List<DSMNode> dsmNodes = new ArrayList<>();

        Map<Integer, List<String>> partitionGroups = config.getPartitionGroups();
        int totalAddresses = config.getTotalAddresses();
        int numPartitions = partitionGroups.size();
        if (numPartitions == 0) {
            throw new IllegalStateException("PartitionConfig has no partitions.");
        }


        int partitionSize = totalAddresses / numPartitions;

        // For each partition, We compute the address range and assign nodes
        for (Map.Entry<Integer, List<String>> entry : partitionGroups.entrySet()) {
            int partitionID = entry.getKey();
            List<String> group = entry.getValue();

            // Start/end addresses for this partition
            int start = partitionID * partitionSize;
            // If this is the last partition, take all remaining addresses
            int end = (partitionID == numPartitions - 1)
                    ? (totalAddresses - 1)
                    : (start + partitionSize - 1);

            // The first node in 'group' is primary, others are replicas
            for (int i = 0; i < group.size(); i++) {
                String nodeName = group.get(i);
                boolean isPrimary = (i == 0);

                // Replicas are all the other nodes in the same group
                List<String> replicas = new ArrayList<>(group);
                replicas.remove(nodeName);

                // Create the DSMNode and add them to the list of Nodes
                DSMNode dsmNode = new DSMNode(nodeName, start, end, isPrimary, replicas,config,messaging);
                dsmNodes.add(dsmNode);
            }
        }

        return dsmNodes;
    }
}

