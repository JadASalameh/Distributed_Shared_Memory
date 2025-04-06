import com.rabbitmq.client.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Consumer;

public class RabbitMQMessagingService implements MessagingService {
    private final Connection connection;
    private final Channel channel;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public RabbitMQMessagingService() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    public void send(String queue, DSMMessage msg) throws IOException {
        String json = objectMapper.writeValueAsString(msg);
        channel.queueDeclare(queue, false, false, false, null);
        channel.basicPublish("", queue, null, json.getBytes(StandardCharsets.UTF_8));
    }

    public void sendReply(String replyQueue, String value) throws IOException {
        channel.queueDeclare(replyQueue, false, false, false, null);
        channel.basicPublish("", replyQueue, null, value.getBytes(StandardCharsets.UTF_8));
    }

    public void startMessageListener(String queueName, Consumer<DSMMessage> handler) throws IOException {
        channel.queueDeclare(queueName, false, false, false, null);
        channel.basicConsume(queueName, true, (consumerTag, delivery) -> {
            DSMMessage msg = objectMapper.readValue(delivery.getBody(), DSMMessage.class);
            handler.accept(msg);
        }, consumerTag -> {});
    }

    public void close() throws IOException, TimeoutException {
        channel.close();
        connection.close();
    }
}

