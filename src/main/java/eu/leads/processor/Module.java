package eu.leads.processor;

import com.google.common.util.concurrent.AbstractExecutionThreadService;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageListener;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/2/13
 * Time: 7:04 AM
 * To change this template use File | Settings | File Templates.
 */
//basic class for modules.
// it contains a communication Compoonnet for interaction with other modules through JMS.
// and its state is handled by the AbstractExecutionThreadService
//when started a new thread is spawned that runs the run function.
public abstract class Module extends AbstractExecutionThreadService implements MessageListener, ExceptionListener {
    protected CommunicationComponent com;
    protected String name;

    public Module(String url, String name) throws Exception {
        this.name = name;
        com = new CommunicationComponent(url, name);
        com.getConnection().setExceptionListener(this);
    }

    public CommunicationComponent getCom() {
        return com;
    }

    public void setCom(CommunicationComponent com) {
        this.com = com;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void onException(JMSException exception) {
        System.out.println(exception.toString());
        exception.printStackTrace();
    }

    @Override
    protected void startUp() {

        try {
            com.enable();
        } catch (JMSException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void triggerShutdown() {
        try {
            super.stopAsync();
            com.disable();
        } catch (JMSException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

}
