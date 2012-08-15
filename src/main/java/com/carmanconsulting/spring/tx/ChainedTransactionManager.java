package com.carmanconsulting.spring.tx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * @author mh
 * @since 14.02.11
 */
public class ChainedTransactionManager implements PlatformTransactionManager
{

    private final static Logger logger = LoggerFactory.getLogger(ChainedTransactionManager.class);

    private final List<PlatformTransactionManager> transactionManagers;
    private final SynchronizationManager synchronizationManager;

    public ChainedTransactionManager(PlatformTransactionManager... transactionManagers)
    {
        this(new DefaultSynchronizationManager(), transactionManagers);
    }

    public ChainedTransactionManager(SynchronizationManager synchronizationManager, PlatformTransactionManager... transactionManagers)
    {
        this.synchronizationManager = synchronizationManager;
        this.transactionManagers = asList(transactionManagers);
    }

    @Override
    public MultiTransactionStatus getTransaction(TransactionDefinition definition) throws TransactionException
    {

        MultiTransactionStatus mts = new MultiTransactionStatus(transactionManagers.get(0)/*First TM is main TM*/);

        if (!synchronizationManager.isSynchronizationActive())
        {
            synchronizationManager.initSynchronization();
            mts.setNewSynchonization();
        }

        for (PlatformTransactionManager transactionManager : transactionManagers)
        {
            mts.registerTransactionManager(definition, transactionManager);
        }

        return mts;
    }

    @Override
    public void commit(TransactionStatus status) throws TransactionException
    {

        MultiTransactionStatus multiTransactionStatus = (MultiTransactionStatus) status;

        boolean commit = true;
        Exception commitException = null;
        PlatformTransactionManager commitExceptionTransactionManager = null;

        for (PlatformTransactionManager transactionManager : reverse(transactionManagers))
        {
            if (commit)
            {
                try
                {
                    multiTransactionStatus.commit(transactionManager);
                }
                catch (Exception ex)
                {
                    commit = false;
                    commitException = ex;
                    commitExceptionTransactionManager = transactionManager;
                }
            }
            else
            {
                //after unsucessfull commit we must try to rollback remaining transaction managers
                try
                {
                    multiTransactionStatus.rollback(transactionManager);
                }
                catch (Exception ex)
                {
                    logger.warn("Rollback exception (after commit) (" + transactionManager + ") " + ex.getMessage(), ex);
                }
            }
        }

        if (multiTransactionStatus.isNewSynchonization())
        {
            synchronizationManager.clearSynchronization();
        }

        if (commitException != null)
        {
            boolean firstTransactionManagerFailed = commitExceptionTransactionManager == getLastTransactionManager();
            int transactionState = firstTransactionManagerFailed ? HeuristicCompletionException.STATE_ROLLED_BACK : HeuristicCompletionException.STATE_MIXED;
            throw new HeuristicCompletionException(transactionState, commitException);
        }

    }

    @Override
    public void rollback(TransactionStatus status) throws TransactionException
    {

        Exception rollbackException = null;
        PlatformTransactionManager rollbackExceptionTransactionManager = null;


        MultiTransactionStatus multiTransactionStatus = (MultiTransactionStatus) status;

        for (PlatformTransactionManager transactionManager : reverse(transactionManagers))
        {
            try
            {
                multiTransactionStatus.rollback(transactionManager);
            }
            catch (Exception ex)
            {
                if (rollbackException == null)
                {
                    rollbackException = ex;
                    rollbackExceptionTransactionManager = transactionManager;
                }
                else
                {
                    logger.warn("Rollback exception (" + transactionManager + ") " + ex.getMessage(), ex);
                }
            }
        }

        if (multiTransactionStatus.isNewSynchonization())
        {
            synchronizationManager.clearSynchronization();
        }

        if (rollbackException != null)
        {
            throw new UnexpectedRollbackException("Rollback exception, originated at (" + rollbackExceptionTransactionManager + ") " +
                    rollbackException.getMessage(), rollbackException);
        }
    }

    private <T> Iterable<T> reverse(Collection<T> collection)
    {
        List<T> list = new ArrayList<T>(collection);
        Collections.reverse(list);
        return list;
    }


    private PlatformTransactionManager getLastTransactionManager()
    {
        return transactionManagers.get(lastTransactionManagerIndex());
    }

    private int lastTransactionManagerIndex()
    {
        return transactionManagers.size() - 1;
    }

}