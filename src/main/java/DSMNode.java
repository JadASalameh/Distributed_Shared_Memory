import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;



public class DSMNode {
    private final String name;
    private final int startAddress;
    private final int endAddress;
    private final Map<Integer, Integer> storage;
    private final boolean isPrimary;
    private final List<String> replicaNodes;
    private static final long REPLICATION_DELAY_MS = 0; // 100 milliseconds delay



    private PartitionConfig partitionConfig;
    private MessagingService messagingService;

    // Add this nested class inside DSMNode.java
    private static class WriteKey {
        private final Address address;
        private final long sequenceNumber;

        public WriteKey(Address address, long sequenceNumber) {
            this.address = address;
            this.sequenceNumber = sequenceNumber;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WriteKey writeKey = (WriteKey) o;
            return sequenceNumber == writeKey.sequenceNumber && address.equals(writeKey.address);
        }

        @Override
        public int hashCode() {
            return Objects.hash(address, sequenceNumber);
        }
    }

    private final Map<WriteKey, Integer> pendingReplications = new ConcurrentHashMap<>();
    private final Map<WriteKey, String> pendingClientReplies = new ConcurrentHashMap<>();

    @JsonCreator
    public DSMNode(
            @JsonProperty("name") String name,
            @JsonProperty("startAddress") int startAddress,
            @JsonProperty("endAddress") int endAddress,
            @JsonProperty("primary") boolean isPrimary,
            @JsonProperty("replicaNodes") List<String> replicaNodes) {
        this.name = name;
        this.startAddress = startAddress;
        this.endAddress = endAddress;
        this.isPrimary = isPrimary;
        this.replicaNodes = replicaNodes;
        this.storage = new HashMap<>();
        // Set these to null initially; they will be injected later.
        this.partitionConfig = null;
        this.messagingService = null;
        initializeStorage();
    }

    private void initializeStorage() {
        for (int addr = startAddress; addr <= endAddress; addr++) {
            storage.put(addr, 0);
        }
    }

    // Setter to inject PartitionConfig after deserialization.
    public void setPartitionConfig(PartitionConfig config) {
        this.partitionConfig = config;
    }

    // Setter to inject MessagingService after deserialization.
    public void setMessagingService(MessagingService messaging) {
        this.messagingService = messaging;
    }

    public boolean isLocalAddress(Address address) {
        int addrValue = address.getValue();
        return addrValue >= startAddress && addrValue <= endAddress;
    }

    private void handleMessage(DSMMessage msg) {
        try {
            if (!isLocalAddress(msg.getAddress())) {
                System.out.println("[" + name + "] Forwarding " + msg.getType() + " for address " + msg.getAddress().getValue());
                forwardMessage(msg);
                return;
            }

            switch (msg.getType()) {
                case WRITE -> {
                    if (isPrimary) {
                        handleWrite(msg);
                    } else {
                        forwardMessage(msg); // Forward to primary if not primary
                    }
                }
                case READ -> handleRead(msg);


                case REPLICATE -> handleReplicate(msg);
                case REPLICATE_ACK -> handleReplicateAck(msg);
            }
        } catch (Exception e) {
            System.err.println("Error handling message: " + e.getMessage());
        }
    }

    private final AtomicLong latestSequenceNumber = new AtomicLong(0); // Track the latest sequence number
    private final Map<Long, Runnable> pendingReads = new ConcurrentHashMap<>(); // For delayed reads

    // In DSMNode.java, modify handleWrite():
    private void handleWrite(DSMMessage msg) {
        long sequenceNumber = latestSequenceNumber.incrementAndGet();
        int value = Integer.parseInt(msg.getValue());
        storage.put(msg.getAddress().getValue(), value);
        System.out.println("[" + name + "] WROTE value " + msg.getValue() + " at address " + msg.getAddress().getValue());

        if (isPrimary) {
            Address address = msg.getAddress();
            WriteKey key = new WriteKey(address, sequenceNumber); // Unique key for this write
            pendingReplications.put(key, replicaNodes.size()); // Track replicas for THIS write

            if (msg.getReplyToQueue() != null) {
                pendingClientReplies.put(key, msg.getReplyToQueue()); // Track reply queue for THIS write
            }

            replicaNodes.forEach(replica -> {
                System.out.println("[" + name + "] Sending REPLICATE for seq=" + sequenceNumber + " to " + replica);
                DSMMessage replicateMessage = new DSMMessage(
                        DSMMessage.Type.REPLICATE, address, msg.getValue(), this.name, sequenceNumber
                );
                try {
                    messagingService.send(replica, replicateMessage);
                } catch (IOException e) {
                    System.out.println("Replication failed to " + replica + ": " + e.getMessage());
                }
            });
        }
    }

    // In DSMNode.java, modify decrementPendingReplications():
    private void decrementPendingReplications(Address address, long sequenceNumber) {
        WriteKey key = new WriteKey(address, sequenceNumber);
        synchronized (pendingReplications) {
            int remaining = pendingReplications.getOrDefault(key, 0) - 1;
            if (remaining <= 0) {
                pendingReplications.remove(key);
                // Send acknowledgment to client
                String replyQueue = pendingClientReplies.remove(key); // Use WriteKey to get the correct reply
                if (replyQueue != null) {
                    try {
                        messagingService.sendReply(replyQueue, "ACK");
                    } catch (IOException e) {
                        System.err.println("Failed to send ACK to client: " + e.getMessage());
                    }
                }
            } else {
                pendingReplications.put(key, remaining);
            }
        }
    }

    // Update handleReplicateAck() to pass the sequence number:
    private void handleReplicateAck(DSMMessage msg) {
        decrementPendingReplications(msg.getAddress(), msg.getSequenceNumber());
    }



    private void handleRead(DSMMessage msg) {
        if (msg.getSequenceNumber() > latestSequenceNumber.get()){
            System.out.println("[" + name + "] Queuing READ for seq=" + msg.getSequenceNumber());
            pendingReads.put(msg.getSequenceNumber(), () -> {
                Integer value = storage.get(msg.getAddress().getValue());
                if (msg.getReplyToQueue() != null) {
                    try {
                        messagingService.sendReply(msg.getReplyToQueue(), String.valueOf(value));
                        System.out.println("[" + name + "] READ value " + value + " from address " + msg.getAddress().getValue());
                    } catch (IOException e) {
                        System.err.println("Failed to send reply: " + e.getMessage());
                    }
                }
            });
        }else{
            Integer value = storage.get(msg.getAddress().getValue());
            if (msg.getReplyToQueue() != null) {
                try {
                    messagingService.sendReply(msg.getReplyToQueue(), String.valueOf(value));
                    System.out.println("[" + name + "] READ value " + value + " from address " + msg.getAddress().getValue());
                } catch (IOException e) {
                    System.err.println("Failed to send reply: " + e.getMessage());
                }
            }
        }
    }

    private void updateSequenceNumber(long newSequenceNumber) {
        System.out.println("[" + name + "] Updating sequence number to: " + newSequenceNumber);
        latestSequenceNumber.set(newSequenceNumber);
        pendingReads.entrySet().removeIf(entry -> {
            if (entry.getKey() <= latestSequenceNumber.get()) {
                System.out.println("[" + name + "] Processing pending READ for seq=" + entry.getKey());
                entry.getValue().run();
                return true;
            }
            return false;
        });
    }

    private void handleReplicate(DSMMessage msg) {
        System.out.println("[" + name + "] Processing REPLICATE for seq=" + msg.getSequenceNumber());
        int value = Integer.parseInt(msg.getValue());
        storage.put(msg.getAddress().getValue(), value);
        // Update sequence number first
        latestSequenceNumber.updateAndGet(current ->
                Math.max(current, msg.getSequenceNumber())
        );
        // Process pending reads with the new sequence number
        updateSequenceNumber(latestSequenceNumber.get());

        // Send ACK back to primary
        DSMMessage ackMsg = new DSMMessage(
                DSMMessage.Type.REPLICATE_ACK,
                msg.getAddress(),
                "ACK",
                msg.getReplyToQueue(),
                msg.getSequenceNumber()
        );
        try {
            messagingService.send(msg.getReplyToQueue(), ackMsg);
        } catch (IOException e) {
            System.err.println("Failed to send REPLICATE_ACK: " + e.getMessage());
        }
    }

    private void forwardMessage(DSMMessage msg) {
        List<String> group = partitionConfig.getReplicationGroup(msg.getAddress());
        String targetNode;
        if (msg.getType() == DSMMessage.Type.READ) {
            if (group.size() > 1) {
                List<String> replicas = group.subList(1, group.size());
                targetNode = replicas.get(new Random().nextInt(replicas.size()));
            } else {
                // Fallback: if there's only one node, it’s effectively acting as both primary and replica
                targetNode = group.get(1);
            }
//            targetNode = group.get(1);
        } else {
            targetNode = group.get(0);
        }
        try {
            messagingService.send(targetNode, msg);
        } catch (IOException e) {
            System.err.println("Failed to forward message to " + targetNode + ": " + e.getMessage());
        }
    }


    public void start() throws IOException {
        messagingService.startMessageListener(this.name, this::handleMessage);
        System.out.println("DSMNode " + name + " is now listening for messages.");
    }

    // Getters
    public String getName() { return name; }
    public int getStartAddress() { return startAddress; }
    public int getEndAddress() { return endAddress; }
    public boolean isPrimary() { return isPrimary; }
    public List<String> getReplicaNodes() { return replicaNodes; }
}

