package com.carmanconsulting.spring.tx;

import org.apache.activemq.pool.PooledConnectionFactory;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.managed.BasicManagedDataSource;
import org.h2.Driver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.objectweb.jotm.Jotm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCountCallbackHandler;
import org.springframework.jms.connection.JmsTransactionManager;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.jta.JtaTransactionManager;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import static org.junit.Assert.*;

public class TestChainedTransactionManager
{
//
// Fields
//

    public static final String TEST_QUEUE = "testQueue";
    private static final Logger logger = LoggerFactory.getLogger(TestChainedTransactionManager.class);

    private BasicDataSource ds1;
    private BasicDataSource ds2;
    private BasicDataSource ds3;
    private Jotm jotm;
    private PooledConnectionFactory connectionFactory;
    private JtaTransactionManager jtaTransactionManager;
    private JmsTransactionManager jmsTransactionManager;
    private ChainedTransactionManager chainedTransactionManager;

//
// Other Methods
//

    @After
    public void closeConnections()
    {
        closeDataSource(ds1);
        closeDataSource(ds2);
        closeDataSource(ds3);
        connectionFactory.stop();
    }

    private void closeDataSource(BasicDataSource ds)
    {
        try
        {
            ds.close();
        }
        catch (SQLException e)
        {
            logger.error("Unable to close DataSource.", e);
        }
    }

    @Before
    public void setUpConnections() throws Exception
    {
        jotm = new Jotm(true, false);

        connectionFactory = new PooledConnectionFactory("vm://" + getClass().getSimpleName() + "?broker.persistent=false&broker.useJmx=false");
        connectionFactory.setMaxConnections(1);
        connectionFactory.setMaximumActive(1);
        connectionFactory.start();
        ds1 = createDataSource();
        ds2 = createDataSource();
        ds3 = createDataSource();
        jtaTransactionManager = new JtaTransactionManager(jotm.getTransactionManager());

        jmsTransactionManager = new JmsTransactionManager(connectionFactory);
        chainedTransactionManager = new ChainedTransactionManager(jtaTransactionManager, jmsTransactionManager);
    }

    private BasicDataSource createDataSource()
    {
        BasicManagedDataSource ds = new BasicManagedDataSource();
        ds.setDriverClassName(Driver.class.getName());
        ds.setUrl("jdbc:h2:mem:" + UUID.randomUUID().toString());
        ds.setUsername("sa");
        ds.setPassword("");
        ds.setTransactionManager(jotm.getTransactionManager());
        JdbcTemplate template = new JdbcTemplate(ds);
        template.update("create table test_table (id int, value varchar(255))");
        return ds;
    }

    @Before
    public void setUpJtaTransactionManager() throws Exception
    {

    }

    @Test
    public void testCommit()
    {
        TransactionTemplate txTemplate = new TransactionTemplate(chainedTransactionManager);

        txTemplate.execute(new TransactionCallback<Void>()
        {
            @Override
            public Void doInTransaction(TransactionStatus status)
            {
                new JmsTemplate(connectionFactory).convertAndSend("testQueue", "Hello, World!");
                executeUpdate("insert into test_table values (1, 'hello')", ds1);
                executeUpdate("insert into test_table values (2, 'world')", ds2);
                return null;
            }
        });
        assertEquals(1, getRowCount(ds1));
        assertEquals(1, getRowCount(ds2));
        assertEquals("Hello, World!", getMessageFromQueue(TEST_QUEUE));
    }

    private void executeUpdate(String sql, DataSource dataSource)
    {
        Connection connection = null;
        Statement statement = null;
        try
        {
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            statement.executeUpdate(sql);
        }
        catch (SQLException e)
        {
            throw new RuntimeException("Unable to execute update.", e);
        }
        finally
        {
            closeStatement(statement);
            closeConnection(connection);
        }
    }

    private void closeStatement(Statement statement)
    {
        try
        {
            if (statement != null)
            {
                statement.close();
            }
        }
        catch (SQLException e)
        {
            logger.error("Unable to close statement.", e);
        }
    }

    private void closeConnection(Connection connection)
    {
        try
        {
            if (connection != null)
            {
                connection.close();
            }
        }
        catch (SQLException e)
        {
            logger.error("Unable to close connection.", e);
        }
    }

    private int getRowCount(DataSource ds)
    {
        RowCountCallbackHandler handler = new RowCountCallbackHandler();
        new JdbcTemplate(ds).query("select * from test_table", handler);
        return handler.getRowCount();
    }

    private String getMessageFromQueue(String queueName)
    {
        try
        {
            JmsTemplate template = new JmsTemplate(connectionFactory);
            template.setReceiveTimeout(10);

            Message message = template.receive(queueName);
            if (message instanceof TextMessage)
            {
                return ((TextMessage) message).getText();
            }
            return null;
        }
        catch (JMSException e)
        {
            throw new RuntimeException("Unable to retrieve message.", e );
        }
    }

    @Test
    public void testRollback()
    {
        TransactionTemplate txTemplate = new TransactionTemplate(chainedTransactionManager);
        try
        {
            txTemplate.execute(new TransactionCallback<Void>()
            {
                @Override
                public Void doInTransaction(TransactionStatus status)
                {
                    new JmsTemplate(connectionFactory).convertAndSend("testQueue", "Hello, World!");
                    executeUpdate("insert into test_table values (1, 'hello')", ds1);
                    executeUpdate("insert into test_table values (2)", ds2);

                    return null;
                }
            });
            fail("Should throw an exception!");
        }
        catch (RuntimeException e)
        {
            // Do nothing, expected exception!
        }
        assertNoRowsInTable(ds1);
        assertNoMessagesInQueue(TEST_QUEUE);
    }

    private void assertNoRowsInTable(DataSource ds)
    {
        int rowCount = getRowCount(ds);
        assertEquals(0, rowCount);
    }

    private void assertNoMessagesInQueue(String queueName)
    {
        assertNull(getMessageFromQueue(queueName));
    }
}
