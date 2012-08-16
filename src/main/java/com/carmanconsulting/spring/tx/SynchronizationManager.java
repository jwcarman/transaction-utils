package com.carmanconsulting.spring.tx;

/**
 * @author mh
 * @since 15.02.11
 */
public interface SynchronizationManager
{
//
// Other Methods
//

    void clearSynchronization();
    void initSynchronization();

    boolean isSynchronizationActive();
}
