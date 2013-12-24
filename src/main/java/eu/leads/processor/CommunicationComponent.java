package eu.leads.processor;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/7/13
 * Time: 6:48 PM
 * .
 */
//CommunicationComponent is used from modules to exchange JMS messages either using a queue or a topic
//We use apache ActiveMQ as a JMS Broker
public class CommunicationComponent {
    private String identifier;
    private Map<String, MessageProducer> topicProducers;
    private Map<String, MessageConsumer> topicConsumers;
    private Map<String, MessageProducer> queueProducers;
    private Map<String, MessageConsumer> queueConsumers;
    private Map<String, Topic> consumerTopics;
    private Map<String, Topic> producerTopics;
    private Map<String, Queue> consumerQueues;
    private Map<String, Queue> producerQueues;
    private Topic control;
    private String url;
    private Session session;
    private Connection connection;


    public CommunicationComponent(String url, String identifier) throws JMSException, NamingException {
        this.identifier = identifier;
        topicProducers = new HashMap<String, MessageProducer>();
        topicConsumers = new HashMap<String, MessageConsumer>();
        queueProducers = new HashMap<String, MessageProducer>();
        queueConsumers = new HashMap<String, MessageConsumer>();
        consumerTopics = new HashMap<String, Topic>();
        producerTopics = new HashMap<String, Topic>();
        consumerQueues = new HashMap<String, Queue>();
        producerQueues = new HashMap<String, Queue>();
        Properties props = new Properties();
        props.setProperty(Context.INITIAL_CONTEXT_FACTORY,
                "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
        props.setProperty(Context.PROVIDER_URL, url);
        javax.naming.Context ctx = new InitialContext(props);
        ConnectionFactory connectionFactory = (ConnectionFactory) ctx.lookup("ConnectionFactory");
        connection = connectionFactory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        control = session.createTopic("leads.processor.control");
        subscribeToTopic(control);
        MessageProducer defaultProducer = session.createProducer(null);
        queueProducers.put("default", defaultProducer);
    }

    private void subscribeToTopic(Topic topic) throws JMSException {
        if (!consumerTopics.containsKey(topic.getTopicName())) {
            MessageConsumer newconsumer = session.createConsumer(topic);
            consumerTopics.put(topic.getTopicName(), topic);
            topicConsumers.put(topic.getTopicName(), newconsumer);
        }
    }

    public void subscribeToTopic(String topic) throws JMSException {
        if (!consumerTopics.containsKey(topic)) {
            Topic newtopic = session.createTopic(topic);
            MessageConsumer newconsumer = session.createConsumer(newtopic);
            consumerTopics.put(topic, newtopic);
            topicConsumers.put(topic, newconsumer);
        }

    }

    public void unsubribeFromTopic(String topic) throws JMSException {
        if (consumerTopics.containsKey(topic)) {
            session.unsubscribe(topic);
            consumerTopics.remove(topic);
            topicConsumers.remove(topic);
        }
    }

    public void subscribeToQueue(String queue) throws JMSException {
        if (!consumerQueues.containsKey(queue)) {
            Queue newqueue = session.createQueue(queue);
            MessageConsumer newconsumer = session.createConsumer(newqueue);
            consumerQueues.put(queue, newqueue);
            queueConsumers.put(queue, newconsumer);
        }
    }

    public void subscribeToQueue(Destination destination) throws JMSException {
        if (!consumerQueues.containsKey(destination.toString())) {
            MessageConsumer newconsumer = session.createConsumer(destination);
            consumerQueues.put(destination.toString(), (Queue) destination);
            queueConsumers.put(destination.toString(), newconsumer);
        }
    }

    public void unsubscribeFromQueue(String queue) throws JMSException {
        if (consumerQueues.containsKey(queue)) {
            session.unsubscribe(queue);
            consumerQueues.remove(queue);
            queueConsumers.remove(queue);
        }

    }

    public void createTopicPublisher(String topic) throws JMSException {
        Topic newtopic = session.createTopic(topic);
        MessageProducer newproducer = session.createProducer(newtopic);
        producerTopics.put(topic, newtopic);
        topicProducers.put(topic, newproducer);
    }

    public void createQueuePublisher(String queue) throws JMSException {
        Queue newqueue = session.createQueue(queue);
        MessageProducer newproducer = session.createProducer(newqueue);
        producerQueues.put(queue, newqueue);
        queueProducers.put(queue, newproducer);
    }

    public void publishToTopic(Message message, String topic) throws JMSException {
        MessageProducer producer = topicProducers.get(topic);
        if (producer != null)
            producer.send(message);
    }

    public void publishToQueue(Message message, String queue) throws JMSException {
        MessageProducer producer = queueProducers.get(queue);
        if (producer != null)
            producer.send(message);
    }

    public void removeTopicPublisher(String topic) {
        if (producerTopics.containsKey(topic)) {
            producerTopics.remove(topic);
            topicProducers.remove(topic);
        }
    }

    public void removeQueuePublisher(String queue) {
        if (producerQueues.containsKey(queue)) {
            producerQueues.remove(queue);
            queueProducers.remove(queue);
        }
    }

    public void setTopicMessageListener(MessageListener listener, String queue) throws JMSException {
        if (topicConsumers.containsKey(queue)) {
            MessageConsumer consumer = topicConsumers.get(queue);
            consumer.setMessageListener(listener);
        }
    }

    public void setTopicMessageListener(MessageListener listener) throws JMSException {
        for (java.util.Iterator<MessageConsumer> iterator = topicConsumers.values().iterator(); iterator.hasNext(); ) {
            MessageConsumer next = iterator.next();
            next.setMessageListener(listener);
        }
    }

    public void setQueueMessageListener(MessageListener listener, String queue) throws JMSException {
        if (queueConsumers.containsKey(queue)) {
            MessageConsumer consumer = queueConsumers.get(queue);
            consumer.setMessageListener(listener);
        }
    }

    public void setQueueMessageListener(MessageListener listener) throws JMSException {
        for (java.util.Iterator<MessageConsumer> iterator = queueConsumers.values().iterator(); iterator.hasNext(); ) {
            MessageConsumer next = iterator.next();
            next.setMessageListener(listener);
        }
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public Map<String, MessageProducer> getTopicProducers() {
        return topicProducers;
    }

    public void setTopicProducers(Map<String, MessageProducer> topicProducers) {
        this.topicProducers = topicProducers;
    }

    public Map<String, MessageConsumer> getTopicConsumers() {
        return topicConsumers;
    }

    public void setTopicConsumers(Map<String, MessageConsumer> topicConsumers) {
        this.topicConsumers = topicConsumers;
    }

    public Map<String, MessageProducer> getQueueProducers() {
        return queueProducers;
    }

    public void setQueueProducers(Map<String, MessageProducer> queueProducers) {
        this.queueProducers = queueProducers;
    }

    public Map<String, MessageConsumer> getQueueConsumers() {
        return queueConsumers;
    }

    public void setQueueConsumers(Map<String, MessageConsumer> queueConsumers) {
        this.queueConsumers = queueConsumers;
    }

    public Map<String, Topic> getConsumerTopics() {
        return consumerTopics;
    }

    public void setConsumerTopics(Map<String, Topic> consumerTopics) {
        this.consumerTopics = consumerTopics;
    }

    public Map<String, Topic> getProducerTopics() {
        return producerTopics;
    }

    public void setProducerTopics(Map<String, Topic> producerTopics) {
        this.producerTopics = producerTopics;
    }

    public Map<String, Queue> getConsumerQueues() {
        return consumerQueues;
    }

    public void setConsumerQueues(Map<String, Queue> consumerQueues) {
        this.consumerQueues = consumerQueues;
    }

    public Map<String, Queue> getProducerQueues() {
        return producerQueues;
    }

    public void setProducerQueues(Map<String, Queue> producerQueues) {
        this.producerQueues = producerQueues;
    }

    public Topic getControl() {
        return control;
    }

    public void setControl(Topic control) {
        this.control = control;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public void enable() throws JMSException {
        connection.start();

    }

    public void disable() throws JMSException {
        session.close();
        connection.close();
    }

    public void publishToDestination(TextMessage reply, String location) {
        try {
            Destination destination = session.createQueue(location);
            queueProducers.get("default").send(destination, reply);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }


}
