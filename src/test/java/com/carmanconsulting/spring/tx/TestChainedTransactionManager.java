package com.carmanconsulting.spring.tx;

import org.apache.activemq.ActiveMQConnectionFactory;
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

import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestChainedTransactionManager
{
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger logger = LoggerFactory.getLogger(TestChainedTransactionManager.class);
    public static final String TEST_QUEUE = "testQueue";

    private BasicDataSource ds1;
    private BasicDataSource ds2;
    private BasicDataSource ds3;
    private Jotm jotm;
    private ConnectionFactory connectionFactory;

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @After
    public void closeConnections()
    {
        closeDataSource(ds1);
        closeDataSource(ds2);
        closeDataSource(ds3);
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
        connectionFactory = new ActiveMQConnectionFactory("vm:" + getClass().getSimpleName() + "?persistent=false&useJmx=false");
        ds1 = createDataSource();
        ds2 = createDataSource();
        ds3 = createDataSource();
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
        catch(SQLException e)
        {
            throw new RuntimeException("Unable to execute update.", e);
        }
        finally
        {
            closeStatement(statement);
            closeConnection(connection);
        }
    }

    private void closeConnection(Connection connection)
    {
        try
        {
            if(connection != null)
            {
                connection.close();
            }
        }
        catch(SQLException e)
        {
            logger.error("Unable to close connection.", e);
        }
    }
    private void closeStatement(Statement statement)
    {
        try
        {
            if(statement != null)
            {
                statement.close();
            }
        }
        catch(SQLException e)
        {
            logger.error("Unable to close statement.", e );
        }
    }

    @Before
    public void setUpJtaTransactionManager() throws Exception
    {

    }

    @Test
    public void testDistributedTransactionRollback()
    {
        final JtaTransactionManager jtaTransactionManager = new JtaTransactionManager(jotm.getTransactionManager());
        final JmsTransactionManager jmsTransactionManager = new JmsTransactionManager(connectionFactory);
        final ChainedTransactionManager transactionManager = new ChainedTransactionManager(jtaTransactionManager, jmsTransactionManager);
        TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);

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
        }
        catch(RuntimeException e)
        {
            logger.error("Caught exception!", e);
        }
        RowCountCallbackHandler handler = new RowCountCallbackHandler();
        new JdbcTemplate(ds1).query("select * from test_table", handler);
        assertEquals(0, handler.getRowCount());
        JmsTemplate template = new JmsTemplate(connectionFactory);
        template.setReceiveTimeout(250);
        Message message = template.receive(TEST_QUEUE);
        assertNull(message);
    }
}
