package com.carmanconsulting.spring.tx;

import org.apache.commons.dbcp.BasicDataSource;
import org.h2.Driver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCountCallbackHandler;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.SQLException;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TestChainedTransactionManager
{
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger logger = LoggerFactory.getLogger(TestChainedTransactionManager.class);

    private BasicDataSource ds1;
    private BasicDataSource ds2;
    private DataSourceTransactionManager tx1;
    private DataSourceTransactionManager tx2;
    private BasicDataSource ds3;
    private DataSourceTransactionManager tx3;

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
    public void setUpConnections()
    {
        ds1 = createDataSource();
        ds2 = createDataSource();
        ds3 = createDataSource();
        tx1 = new DataSourceTransactionManager(ds1);
        tx2 = new DataSourceTransactionManager(ds2);
        tx3 = new DataSourceTransactionManager(ds3);
    }

    private BasicDataSource createDataSource()
    {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(Driver.class.getName());
        ds.setUrl("jdbc:h2:mem:" + UUID.randomUUID().toString());
        ds.setUsername("sa");
        ds.setPassword("");
        JdbcTemplate template = new JdbcTemplate(ds);
        template.update("create table test_table (id int, value varchar(255))");
        return ds;
    }

    @Test
    public void testDistributedTransactionRollback()
    {
        final ChainedTransactionManager transactionManager = new ChainedTransactionManager(tx1, tx2, tx3);
        TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
        try
        {
            txTemplate.execute(new TransactionCallback<Void>()
            {
                @Override
                public Void doInTransaction(TransactionStatus transactionStatus)
                {
                    new JdbcTemplate(ds1).update("insert into test_table values (1,'hello')");
                    new JdbcTemplate(ds2).update("insert into test_table values (2)");
                    return null;
                }
            });
        }
        catch (RuntimeException e)
        {
            logger.info("Encountered error.", e);
        }

        RowCountCallbackHandler handler = new RowCountCallbackHandler();
        new JdbcTemplate(ds1).query("select * from test_table", handler);
        assertEquals(0, handler.getRowCount());
    }
}
