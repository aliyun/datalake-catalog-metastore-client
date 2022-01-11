package com.aliyun.datalake.metastore.hive2;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hive.hcatalog.common.HiveClientCache;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Add # of current users on HiveMetaStoreClient, so that the client can be cleaned when no one is using it.
 */
class DLFCacheableMetaStoreClient extends ProxyMetaStoreClient implements HiveClientCache.ICacheableMetaStoreClient {
    private static final Logger logger = LoggerFactory.getLogger(DLFCacheableMetaStoreClient.class);

    private final AtomicInteger users = new AtomicInteger(0);
    private volatile boolean expiredFromCache = false;
    private boolean isClosed = false;

    DLFCacheableMetaStoreClient(final HiveConf conf, final Integer timeout, Boolean allowEmbedded)
            throws MetaException {
        super(conf, null, allowEmbedded);
    }

    /**
     * Increments the user count and optionally renews the expiration time.
     * <code>renew</code> should correspond with the expiration policy of the cache.
     * When the policy is <code>expireAfterAccess</code>, the expiration time should be extended.
     * When the policy is <code>expireAfterWrite</code>, the expiration time should not be extended.
     * A mismatch with the policy will lead to closing the connection unnecessarily after the initial
     * expiration time is generated.
     *
     * @param renew whether the expiration time should be extended.
     */
    public synchronized void acquire() {
        users.incrementAndGet();
        if (users.get() > 1) {
            logger.warn("Unexpected increment of user count beyond one: " + users.get() + " " + this);
        }
    }

    /**
     * Decrements the user count.
     */
    private void release() {
        if (users.get() > 0) {
            users.decrementAndGet();
        } else {
            logger.warn("Unexpected attempt to decrement user count of zero: " + users.get() + " " + this);
        }
    }

    /**
     * Communicate to the client that it is no longer in the cache.
     * The expiration time should be voided to allow the connection to be closed at the first opportunity.
     */
    public synchronized void setExpiredFromCache() {
        if (users.get() != 0) {
            logger.warn("Evicted client has non-zero user count: " + users.get());
        }

        expiredFromCache = true;
    }

    public boolean isClosed() {
        return isClosed;
    }

    /*
     * Used only for Debugging or testing purposes
     */
    public AtomicInteger getUsers() {
        return users;
    }

    /**
     * Make a call to hive meta store and see if the client is still usable. Some calls where the user provides
     * invalid data renders the client unusable for future use (example: create a table with very long table name)
     *
     * @return
     */
    @Deprecated
    public boolean isOpen() {
        try {
            // Look for an unlikely database name and see if either MetaException or TException is thrown
            super.getDatabases("NonExistentDatabaseUsedForHealthCheck");
        } catch (TException e) {
            return false;
        }
        return true;
    }

    /**
     * Decrement the user count and piggyback this to set expiry flag as well, then  teardown(), if conditions are met.
     * This *MUST* be called by anyone who uses this client.
     */
    @Override
    public synchronized void close() {
        release();
        tearDownIfUnused();
    }

    /**
     * Attempt to tear down the client connection.
     * The connection will be closed if the following conditions hold:
     * 1. There are no active user holding the client.
     * 2. The client has been evicted from the cache.
     */
    public synchronized void tearDownIfUnused() {
        if (users.get() != 0) {
            logger.warn("Non-zero user count preventing client tear down: users=" + users.get() + " expired=" + expiredFromCache);
        }

        if (users.get() == 0 && expiredFromCache) {
            this.tearDown();
        }
    }

    /**
     * Close the underlying objects irrespective of whether they are in use or not.
     */
    public void tearDown() {
        try {
            if (!isClosed) {
                super.close();
            }
            isClosed = true;
        } catch (Exception e) {
            logger.warn("Error closing hive metastore client. Ignored.", e);
        }
    }

    @Override
    public String toString() {
        return "HCatClient: thread: " + Thread.currentThread().getId() + " users=" + users.get()
                + " expired=" + expiredFromCache + " closed=" + isClosed;
    }

    /**
     * GC is attempting to destroy the object.
     * No one references this client anymore, so it can be torn down without worrying about user counts.
     *
     * @throws Throwable
     */
    @Override
    protected void finalize() throws Throwable {
        if (users.get() != 0) {
            logger.warn("Closing client with non-zero user count: users=" + users.get() + " expired=" + expiredFromCache);
        }

        try {
            this.tearDown();
        } finally {
            super.finalize();
        }
    }
}
