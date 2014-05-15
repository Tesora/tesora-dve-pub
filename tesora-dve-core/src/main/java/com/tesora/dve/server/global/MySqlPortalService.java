// OS_STATUS: public
package com.tesora.dve.server.global;

/**
 *
 */
public interface MySqlPortalService {
    void setMaxConcurrent(int maxConcurrent);

    int getWorkerGroupCount();

    int getWorkerExecGroupCount();

    int getClientExecutorActiveCount();

    int getClientExecutorCorePoolSize();

    int getClientExecutorPoolSize();

    int getClientExecutorLargestPoolSize();

    int getClientExecutorMaximumPoolSize();

    int getClientExecutorQueueSize();
}
