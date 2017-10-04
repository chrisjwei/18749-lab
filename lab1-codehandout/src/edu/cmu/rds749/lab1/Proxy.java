package edu.cmu.rds749.lab1;

import edu.cmu.rds749.common.AbstractProxy;
import edu.cmu.rds749.common.BankAccountStub;
import org.apache.commons.configuration2.Configuration;
import rds749.NoServersAvailable;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by jiaqi on 8/28/16.
 *
 * Implements the Proxy.
 */
public class Proxy extends AbstractProxy
{
    long heartbeatInterval;
    ReadWriteLock poolRWLock;
    ServerPool pool;

    public class ServerConfig
    {

        public String hostname;
        public int port;
        public long id;

        public ServerConfig(String hostname, int port, long id){
            this.hostname = hostname;
            this.port = port;
            this.id = id;
        }
    }

    /**
     * A threadsafe server list that contains the mappings of serverids to hostname and port and the current active
     * server id
     */
    public class ServerPool {
        HashMap<Long, ServerConfig> servers;
        HashMap<Long, ServerConfig> deadServers;
        long activeServerId;
        long serverIdCounter;
        long expirationTimestamp;

        public ServerPool()
        {
            this.activeServerId = -1;
            this.serverIdCounter = 0;
            this.servers = new HashMap<>();
            this.deadServers = new HashMap<>();
            //TODO: spawn thread to handle server heartbeat expiration
        }

        /**
         * Gets the server config from the list of servers based on an ID
         * @param id the server id
         * @return the server config
         */
        public ServerConfig getServerConfig(long id)
        {
            ServerConfig cfg;
            cfg = this.servers.get(id);
            return cfg;
        }

        /**
         * Gets the current server config from the list of servers
         * @return the ServerConfig of the current active server
         */
        public ServerConfig getCurrentServerConfig()
        {
            ServerConfig cfg;
            if (this.activeServerId == -1)
            {
                return null;
            }
            cfg = this.servers.get(activeServerId);
            return cfg;
        }

        /**
         * Registers a new server with the server pool
         * @param hostname the hostname
         * @param port the port
         * @return the server's unique id
         */
        public long registerServer(String hostname, int port){
            long id = this.serverIdCounter++;
            this.servers.put(id, new ServerConfig(hostname, port, id));
            return id;
        }

        /**
         * Removes the current server from the server pool if the id matches the activeServerId
         * @param id
         */
        public void declareDeadServer(long id){
            // if declaring the current server as dead, select a new active server
            if (this.activeServerId == id){
                if (this.servers.size() == 0)
                {
                    this.activeServerId = -1;
                }
                else
                {
                    this.activeServerId = this.servers.keySet().iterator().next();
                }
            }
            // move the config from live to dead pool
            this.deadServers.put(id, this.servers.remove(id));
        }
    }

    public Proxy(Configuration config)
    {
        super(config);
        this.heartbeatInterval = config.getLong("heartbeatIntervalMillis");
        this.pool = new ServerPool();
        this.poolRWLock = new ReentrantReadWriteLock();
    }

    public int readBalance() throws NoServersAvailable
    {
        int balance;
        ServerConfig cfg;
        BankAccountStub stub;

        while(true)
        {
            this.poolRWLock.writeLock().lock(); // we need a write lock because we could modify serverPool if exception
            cfg = this.pool.getCurrentServerConfig();
            if (cfg == null)
            {
                this.poolRWLock.writeLock().unlock();
                throw new NoServersAvailable();
            }
            stub = this.connectToServer(cfg.hostname, cfg.port);
            try
            {
                balance = stub.readBalance();
            }
            catch (BankAccountStub.NoConnectionException e)
            {
                this.pool.declareDeadServer(cfg.id);
                this.poolRWLock.writeLock().unlock();
                continue;
            }
            this.poolRWLock.writeLock().unlock();
            return balance;
        }
    }

    public int changeBalance(int update) throws NoServersAvailable
    {
        int balance;
        ServerConfig cfg;
        BankAccountStub stub;

        while(true)
        {
            this.poolRWLock.writeLock().lock();
            cfg = this.pool.getCurrentServerConfig();
            if (cfg == null)
            {
                this.poolRWLock.writeLock().unlock();
                throw new NoServersAvailable();
            }
            stub = this.connectToServer(cfg.hostname, cfg.port);
            try
            {
                balance = stub.changeBalance(update);
            }
            catch (BankAccountStub.NoConnectionException e)
            {
                this.pool.declareDeadServer(cfg.id);
                this.poolRWLock.writeLock().unlock();
                continue;
            }
            this.poolRWLock.writeLock().unlock();
            return balance;
        }
    }

    public long register(String hostname, int port)
    {
        this.poolRWLock.writeLock().lock();
        long uid = this.pool.registerServer(hostname, port);
        this.poolRWLock.writeLock().unlock();
        return uid;
    }

    public void heartbeat(long ID, long serverTimestamp)
    {
        //TODO: check if we should be using serverTimestamp or the proxy timestamp?

        //TODO: get the ServerConfig, no matter where it is
        //TODO: if serverTimeStamp > lastTimeStamp, then revive if need be
        //TODO: update with new serverTimeStamp
    }
}
