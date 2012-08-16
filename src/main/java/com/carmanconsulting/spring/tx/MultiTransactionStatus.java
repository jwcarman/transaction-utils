package com.carmanconsulting.spring.tx;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author mh
 * @since 14.02.11
 */
public class MultiTransactionStatus implements TransactionStatus
{
//
// Fields
//

    private PlatformTransactionManager mainTransactionManager;

    private Map<PlatformTransactionManager, TransactionStatus> transactionStatuses =
            Collections.synchronizedMap(new HashMap<PlatformTransactionManager, TransactionStatus>());

    private boolean newSynchonization;

//
// Constructors
//

    public MultiTransactionStatus(PlatformTransactionManager mainTransactionManager)
    {
        this.mainTransactionManager = mainTransactionManager;
    }

//
// SavepointManager Implementation
//

    @Override
    public Object createSavepoint() throws TransactionException
    {
        SavePoints savePoints = new SavePoints();

        for (TransactionStatus transactionStatus : transactionStatuses.values())
        {
            savePoints.save(transactionStatus);
        }
        return savePoints;
    }

    @Override
    public void releaseSavepoint(Object savepoint) throws TransactionException
    {
        ((SavePoints) savepoint).release();
    }

    @Override
    public void rollbackToSavepoint(Object savepoint) throws TransactionException
    {
        SavePoints savePoints = (SavePoints) savepoint;
        savePoints.rollback();
    }

//
// TransactionStatus Implementation
//

    @Override
    public void flush()
    {
        for (TransactionStatus transactionStatus : transactionStatuses.values())
        {
            transactionStatus.flush();
        }
    }

    @Override
    public boolean hasSavepoint()
    {
        return getMainTransactionStatus().hasSavepoint();
    }

    @Override
    public boolean isCompleted()
    {
        return getMainTransactionStatus().isCompleted();
    }

    @Override
    public boolean isNewTransaction()
    {
        return getMainTransactionStatus().isNewTransaction();
    }

    @Override
    public boolean isRollbackOnly()
    {
        return getMainTransactionStatus().isRollbackOnly();
    }

    @Override
    public void setRollbackOnly()
    {
        for (TransactionStatus ts : transactionStatuses.values())
        {
            ts.setRollbackOnly();
        }
    }

//
// Getter/Setter Methods
//

    private Map<PlatformTransactionManager, TransactionStatus> getTransactionStatuses()
    {
        return transactionStatuses;
    }

    public boolean isNewSynchonization()
    {
        return newSynchonization;
    }

//
// Other Methods
//

    void commit(PlatformTransactionManager transactionManager)
    {
        TransactionStatus transactionStatus = getTransactionStatus(transactionManager);
        transactionManager.commit(transactionStatus);
    }

    private TransactionStatus getTransactionStatus(PlatformTransactionManager transactionManager)
    {
        return this.getTransactionStatuses().get(transactionManager);
    }

    private TransactionStatus getMainTransactionStatus()
    {
        return transactionStatuses.get(mainTransactionManager);
    }

    public void registerTransactionManager(TransactionDefinition definition, PlatformTransactionManager transactionManager)
    {
        getTransactionStatuses().put(transactionManager, transactionManager.getTransaction(definition));
    }

    void rollback(PlatformTransactionManager transactionManager)
    {
        transactionManager.rollback(getTransactionStatus(transactionManager));
    }

    public void setNewSynchonization()
    {
        this.newSynchonization = true;
    }

//
// Inner Classes
//

    private static class SavePoints
    {
        Map<TransactionStatus, Object> savepoints = new HashMap<TransactionStatus, Object>();

        private void addSavePoint(TransactionStatus status, Object savepoint)
        {
            this.savepoints.put(status, savepoint);
        }

        private void save(TransactionStatus transactionStatus)
        {
            Object savepoint = transactionStatus.createSavepoint();
            addSavePoint(transactionStatus, savepoint);
        }

        public void rollback()
        {
            for (TransactionStatus transactionStatus : savepoints.keySet())
            {
                transactionStatus.rollbackToSavepoint(savepointFor(transactionStatus));
            }
        }

        private Object savepointFor(TransactionStatus transactionStatus)
        {
            return savepoints.get(transactionStatus);
        }

        public void release()
        {
            for (TransactionStatus transactionStatus : savepoints.keySet())
            {
                transactionStatus.releaseSavepoint(savepointFor(transactionStatus));
            }
        }
    }
}