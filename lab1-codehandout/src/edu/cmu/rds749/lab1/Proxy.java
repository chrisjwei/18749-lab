package edu.cmu.rds749.lab1;

import edu.cmu.rds749.common.AbstractProxy;
import edu.cmu.rds749.common.BankAccountStub;
import org.apache.commons.configuration2.Configuration;
import rds749.NoServersAvailable;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * Created by jiaqi on 8/28/16.
 * <p>
 * Implements the Proxy.
 */
public class Proxy extends AbstractProxy {
    long heartbeatInterval;
    ReadWriteLock poolRWLock;
    ServerPool pool;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public class ServerConfig {
        public String hostname;
        public int port;
        public long id;
        public boolean alive;
        public long lastHeartbeatLocal;
        public long lastHeartbeatRemote;

        public ServerConfig(String hostname, int port, long id) {
            this.hostname = hostname;
            this.port = port;
            this.id = id;
            this.alive = true;
            this.lastHeartbeatLocal = System.currentTimeMillis();
            this.lastHeartbeatRemote = 0;
        }
    }

    /**
     * A threadsafe server list that contains the mappings of serverids to hostname and port and the current active
     * server id
     */
    public class ServerPool {
        HashMap<Long, ServerConfig> servers;
        long activeServerId;
        long serverIdCounter;
        long expirationTimestamp;

        public ServerPool() {
            this.activeServerId = -1;
            this.serverIdCounter = 0;
            this.servers = new HashMap<>();
            //TODO: spawn thread to handle server heartbeat expiration
        }

        /**
         * Gets the server config from the list of servers based on an ID
         *
         * @param id the server id
         * @return the server config
         */
        public ServerConfig getServerConfig(long id) {
            return this.servers.get(id);
        }

        /**
         * Gets the current server config from the list of servers
         *
         * @return the ServerConfig of the current active server
         */
        public ServerConfig getCurrentServerConfig() {
            ServerConfig cfg;
            if (this.activeServerId == -1) {
                return null;
            }
            cfg = this.servers.get(activeServerId);
            return cfg;
        }

        /**
         * Registers a new server with the server pool
         *
         * @param hostname the hostname
         * @param port     the port
         * @return the server's unique id
         */
        public long registerServer(String hostname, int port) {
            long id = this.serverIdCounter++;
            this.servers.put(id, new ServerConfig(hostname, port, id));
            this.declareAliveServer(id);
            return id;
        }

        /**
         * Removes the current server from the server pool if the id matches the activeServerId
         *
         * @param id
         */
        public void declareDeadServer(long id) {
            System.out.printf("Server id %d declared dead%n", id);
            ServerConfig cfg, nextCfg;
            // declare the server dead
            cfg = this.servers.get(id);
            cfg.alive = false;
            // if declaring the current server as dead, select a new active server
            if (this.activeServerId == id) {
                Iterator<ServerConfig> it = this.servers.values().iterator();
                this.activeServerId = -1;
                while (it.hasNext()) {
                    nextCfg = it.next();
                    if (nextCfg.alive) {
                        this.activeServerId = nextCfg.id;
                        break;
                    }
                }
            }
        }

        public void declareAliveServer(long id) {
            System.out.printf("Server id %d declared alive%n", id);
            ServerConfig cfg;
            cfg = this.servers.get(id);
            cfg.alive = true;
            if (this.activeServerId == -1) {
                this.activeServerId = id;
            }
        }
    }

    public Proxy(Configuration config) {
        super(config);
        this.heartbeatInterval = config.getLong("heartbeatIntervalMillis");
        this.pool = new ServerPool();
        this.poolRWLock = new ReentrantReadWriteLock();
        final Proxy p = this;

        this.scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                System.out.println("Checking for expired servers.");

                p.poolRWLock.writeLock().lock();
                Iterator<Long> it = pool.servers.keySet().iterator();
                long expirationTime = System.currentTimeMillis() - 2 * p.heartbeatInterval;
                while (it.hasNext()) {
                    ServerConfig cfg = pool.servers.get(it.next());
                    if (cfg.alive && cfg.lastHeartbeatLocal < expirationTime) {
                        p.pool.declareDeadServer(cfg.id);
                    }
                }
                p.poolRWLock.writeLock().unlock();
            }
        }, 0, this.config.getLong("heartbeatIntervalMillis"), TimeUnit.MILLISECONDS);
    }

    public int readBalance() throws NoServersAvailable {
        int balance;
        ServerConfig cfg;
        BankAccountStub stub;

        while (true) {
            this.poolRWLock.writeLock().lock(); // we need a write lock because we could modify serverPool if exception
            cfg = this.pool.getCurrentServerConfig();
            if (cfg == null) {
                this.poolRWLock.writeLock().unlock();
                throw new NoServersAvailable();
            }
            stub = this.connectToServer(cfg.hostname, cfg.port);
            try {
                balance = stub.readBalance();
            } catch (BankAccountStub.NoConnectionException | NullPointerException e) {
                this.pool.declareDeadServer(cfg.id);
                this.poolRWLock.writeLock().unlock();
                continue;
            }
            this.poolRWLock.writeLock().unlock();
            return balance;
        }
    }

    public int changeBalance(int update) throws NoServersAvailable {
        int balance;
        ServerConfig cfg;
        BankAccountStub stub;

        while (true) {
            this.poolRWLock.writeLock().lock();
            cfg = this.pool.getCurrentServerConfig();
            if (cfg == null) {
                this.poolRWLock.writeLock().unlock();
                throw new NoServersAvailable();
            }
            stub = this.connectToServer(cfg.hostname, cfg.port);
            try {
                balance = stub.changeBalance(update);
            } catch (BankAccountStub.NoConnectionException | NullPointerException e) {
                this.pool.declareDeadServer(cfg.id);
                this.poolRWLock.writeLock().unlock();
                continue;
            }
            this.poolRWLock.writeLock().unlock();
            return balance;
        }
    }

    public long register(String hostname, int port) {
        this.poolRWLock.writeLock().lock();
        long uid = this.pool.registerServer(hostname, port);
        this.poolRWLock.writeLock().unlock();
        System.out.printf("Registered %s:%d with uid %d %n", hostname, port, uid);
        return uid;
    }

    public void heartbeat(long ID, long serverTimestamp) {
        System.out.printf("Got heartbeat from %d with timestamp %d%n", ID, serverTimestamp);
        this.poolRWLock.writeLock().lock();
        ServerConfig cfg = this.pool.getServerConfig(ID);
        // check heartbeat strictly monotonic
        if (cfg.lastHeartbeatRemote < serverTimestamp) {
            if (!cfg.alive) {
                this.pool.declareAliveServer(ID);
            }
            cfg.lastHeartbeatRemote = serverTimestamp;
            cfg.lastHeartbeatLocal = System.currentTimeMillis();
        }
        this.poolRWLock.writeLock().unlock();
    }
}
