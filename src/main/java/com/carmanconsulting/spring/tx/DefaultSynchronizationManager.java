package com.carmanconsulting.spring.tx;

import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * @author mh
 * @since 15.02.11
 */
public class DefaultSynchronizationManager implements SynchronizationManager
{
//
// SynchronizationManager Implementation
//

    @Override
    public void clearSynchronization()
    {
        TransactionSynchronizationManager.clear();
    }

    @Override
    public void initSynchronization()
    {
        TransactionSynchronizationManager.initSynchronization();
    }

    @Override
    public boolean isSynchronizationActive()
    {
        return TransactionSynchronizationManager.isSynchronizationActive();
    }
}