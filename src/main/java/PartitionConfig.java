import java.util.*;

public class PartitionConfig {
    private final int totalAddresses;
    private final int replicationFactor;
    private final List<String> nodeNames;
    private final Map<Integer, List<String>> partitionGroups; // Key=partitionID, Value=[primary, replica1, ...]

    public PartitionConfig(int totalAddresses, int replicationFactor, List<String> nodeNames) {
        this.totalAddresses = totalAddresses;
        this.replicationFactor = replicationFactor;
        this.nodeNames = nodeNames;
        this.partitionGroups = new HashMap<>();
        initializePartitions();
    }

    private void initializePartitions() {
        int numPartitions = nodeNames.size() / (replicationFactor + 1);
        int nodeIndex = 0;
        for (int partitionID = 0; partitionID < numPartitions; partitionID++) {
            List<String> group = new ArrayList<>();
            for (int i = 0; i <= replicationFactor; i++) {
                group.add(nodeNames.get(nodeIndex++));
            }
            partitionGroups.put(partitionID, group);
        }
    }

    // Get the replication group for a given address
    public List<String> getReplicationGroup(Address address) {
        int numPartitions = partitionGroups.size();
        int addressValue = address.getValue();

        if (numPartitions == 0) {
            throw new IllegalStateException("No partitions have been initialized.");
        }

        if(addressValue>=totalAddresses){
            throw new IllegalArgumentException("Address " + addressValue + " is out of bounds (max allowed = " + (totalAddresses - 1) + ")");
        }

        if (totalAddresses == 0) {
            throw new IllegalStateException("Total number of addresses is zero.");
        }

        int partitionSize = totalAddresses / numPartitions;

        // Compute the partition ID and clamp it to the maximum allowed ID
        int partitionID = Math.min(
                addressValue / partitionSize,
                numPartitions - 1
        );

        return partitionGroups.get(partitionID);
    }


    // Get the primary node for an address
    public String getPrimaryNode(Address address) {
        return getReplicationGroup(address).get(0);
    }

    public Map<Integer, List<String>> getPartitionGroups() {
        return partitionGroups;
    }

    public int getTotalAddresses() {
        return totalAddresses;
    }

}