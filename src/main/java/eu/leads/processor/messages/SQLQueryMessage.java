package eu.leads.processor.messages;

import org.apache.activemq.command.ActiveMQTextMessage;

import javax.jms.MessageNotWriteableException;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/7/13
 * Time: 9:19 PM
 * To change this template use File | Settings | File Templates.
 */
public class SQLQueryMessage extends ActiveMQTextMessage {

    String user;
    String location;
    public static final String TYPE = "SQLQueryMessage";

    SQLQueryMessage() {
        super();
    }

    public SQLQueryMessage(String text) {
        try {
            super.setText(text);
        } catch (MessageNotWriteableException e) {
            e.printStackTrace();
        }

    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public String getJMSType() {
        return SQLQueryMessage.TYPE;
    }


}
