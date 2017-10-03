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
    ConcServerPool pool;

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
    public class ConcServerPool {
        ReadWriteLock serversRWLock;
        HashMap<Long, ServerConfig> servers;
        long activeServerId;
        long serverIdCounter;

        public ConcServerPool()
        {
            this.activeServerId = -1;
            this.serverIdCounter = 0;
            this.servers = new HashMap<>();
            this.serversRWLock = new ReentrantReadWriteLock();
        }

        /**
         * Gets the server config from the list of servers based on an ID
         * @param id the server id
         * @return the server config
         */
        public ServerConfig getServerConfig(long id)
        {
            ServerConfig cfg;
            this.serversRWLock.readLock().lock();
            cfg = this.servers.get(id);
            this.serversRWLock.readLock().unlock();
            return cfg;
        }

        /**
         * Gets the current server config from the list of servers
         * @return the ServerConfig of the current active server
         * @throws NoServersAvailable
         */
        public ServerConfig getCurrentServerConfig() throws NoServersAvailable
        {
            ServerConfig cfg;
            this.serversRWLock.readLock().lock();
            if (this.activeServerId == -1)
            {
                this.serversRWLock.readLock().unlock();
                throw new NoServersAvailable();
            }
            cfg = this.servers.get(activeServerId);
            this.serversRWLock.readLock().unlock();
            return cfg;
        }

        /**
         * Registers a new server with the server pool
         * @param hostname the hostname
         * @param port the port
         * @return the server's unique id
         */
        public long registerServer(String hostname, int port){
            this.serversRWLock.writeLock().lock();
            long id = this.serverIdCounter++;
            this.servers.put(id, new ServerConfig(hostname, port, id));
            this.serversRWLock.writeLock().unlock();
            return id;
        }

        /**
         * Removes the current server from the server pool if the id matches the activeServerId
         * @param id
         */
        public void removeServer(long id){
            this.serversRWLock.writeLock().lock();
            if (this.servers.containsKey(id)){
                this.servers.remove(id);
            }
            if (this.activeServerId == id){
                if (this.servers.size() == 0){
                    this.activeServerId = -1;
                }
                else{
                    this.activeServerId = this.servers.keySet().iterator().next();
                }
            }
            this.serversRWLock.writeLock().unlock();
        }
    }

    public Proxy(Configuration config)
    {
        super(config);
        this.heartbeatInterval = config.getLong("heartbeatIntervalMillis");
        this.pool = new ConcServerPool();
    }

    public int readBalance() throws NoServersAvailable
    {
        int balance;
        ServerConfig cfg;
        BankAccountStub stub;

        while(true)
        {
            cfg = this.pool.getCurrentServerConfig();
            stub = this.connectToServer(cfg.hostname, cfg.port);
            try
            {
                balance = stub.readBalance();
            }
            catch (BankAccountStub.NoConnectionException e)
            {
                this.pool.removeServer(cfg.id);
                continue;
            }
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
            cfg = this.pool.getCurrentServerConfig();
            stub = this.connectToServer(cfg.hostname, cfg.port);
            try
            {
                balance = stub.changeBalance(update);
            }
            catch (BankAccountStub.NoConnectionException e)
            {
                this.pool.removeServer(cfg.id);
                continue;
            }
            return balance;
        }
    }

    public long register(String hostname, int port)
    {
        //long uid = UUID.fromString(hostname + Integer.toString(port)).getMostSignificantBits();
        long uid = this.pool.registerServer(hostname, port);
        // TODO: fork off a process to heartbeat
        return uid;
    }

    public void heartbeat(long ID, long serverTimestamp)
    {
        ServerConfig config = this.pool.getServerConfig(ID);
        //TODO: how do you heartbeat?
        //BankAccountStub acc = this.connectToServer(config.hostname, config.port);
    }
}
