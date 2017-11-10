package edu.cmu.rds749.lab3;

import edu.cmu.rds749.common.AbstractProxy;
import edu.cmu.rds749.common.BankAccountStub;
import org.apache.commons.configuration2.Configuration;
import rds749.Checkpoint;
import rds749.IncorrectOperation;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Created by jiaqi on 8/28/16.
 *
 * Implements the Proxy.
 */
public class Proxy extends AbstractProxy
{
    public enum MessageType{
        READ, UPDATE
    }

    public static class Message implements Comparable<Message>{
        public int reqid;
        public MessageType type;
        public int value;

        public int compareTo(Message o){
            return ((Integer)this.reqid).compareTo(o.reqid);
        }

        public Message(MessageType type, int reqid){
            this.type = type;
            this.reqid = reqid;
        }

        public Message(MessageType type, int reqid, int value){
            this.type = type;
            this.reqid = reqid;
            this.value = value;
        }
    }

    public class PendingMessageRecord{
        public Set<Long> serverids;
        public Message message;
        public boolean suppress;

        public PendingMessageRecord(Message message, Set<Long> serverids){
            this.serverids = serverids;
            this.message = message;
            this.suppress = false;
        }

        public boolean reportReception(long serverid){
            boolean removed = this.serverids.remove(serverid);
            assert(removed); // TODO: remove this
            return this.serverids.isEmpty();
        }

        public boolean reportLostMessage(long serverid){
            return this.reportReception(serverid);
        }
    }

    private Lock recordLock;
    private HashMap<Integer,PendingMessageRecord> pendingMessageRecords;

    private Lock serverLock;
    private HashMap<Long, BankAccountStub> servers;
    private long primaryServerId;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public Proxy(Configuration config)
    {
        super(config);
        this.recordLock = new ReentrantLock();
        this.pendingMessageRecords = new HashMap<>();
        this.serverLock = new ReentrantLock();
        this.servers = new HashMap<>();
        this.primaryServerId = -1;
        final Proxy p = this;
        this.scheduler.scheduleAtFixedRate(
            new Runnable() {
                public void run() {
                    Checkpoint c = null;
                    p.serverLock.lock();
                    System.out.println("*** CHECKPOINT START ***");
                    if (p.servers.isEmpty()){
                        p.serverLock.unlock();
                        return;
                    }
                    // get checkpoint from primary server
                    while(c == null){
                        try {
                            System.out.printf("* Getting checkpoint from primary server %d%n", p.primaryServerId);
                            c = p.servers.get(p.primaryServerId).getState();
                        }
                        catch (BankAccountStub.NoConnectionException e){
                            System.out.printf("* Primary server %d has failed %n", p.primaryServerId);
                            // primary has failed, elect new primary
                            p.electNewPrimary();
                            // if no eligible primaries can be elected, return
                            if (p.primaryServerId == -1){
                                p.serverLock.unlock();
                                System.out.println("* No valid primaries could be found");
                                return;
                            }
                            c = null;
                        }
                    }
                    final Checkpoint checkpoint = c;
                    System.out.printf("* Found checkpoint with state %d from server %d%n", c.state, p.primaryServerId);
                    ExecutorService executorService = Executors.newFixedThreadPool(p.servers.keySet().size());
                    final HashMap<Long, BankAccountStub> servers = p.servers;
                    // Generate a list of tasks that return either a failed serverid or null
                    Collection<Callable<Long>> tasks = new ArrayList<>();
                    for (final Long serverid : p.servers.keySet()) {
                        Callable<Long> task = new Callable<Long>() {
                            public Long call() {
                                if (serverid == p.primaryServerId){
                                    return null; // skip the primary server
                                }
                                System.out.printf("* Attempting to send set state to server %d%n", serverid);
                                BankAccountStub stub = servers.get(serverid);
                                assert(stub != null);
                                try{
                                    stub.setState(checkpoint);
                                } catch (BankAccountStub.NoConnectionException e){
                                    System.out.printf("* Server %d threw NoConnectionException%n", serverid);
                                    return serverid;
                                }
                                System.out.printf("* State successfully sent to server %d %n", serverid);
                                return null;
                            }
                        };
                        tasks.add(task);
                    }

                    // Collect all invalidServerIds from the result of the executor
                    HashSet<Long> invalidServerIds = new HashSet<>();
                    List<Future<Long>> results;
                    try {
                        results = executorService.invokeAll(tasks);
                        for (Future<Long> result : results){
                            Long l = null;
                            try { l = result.get(); } catch (ExecutionException e){}
                            if (l != null){
                                invalidServerIds.add(l);
                            }
                        }
                    } catch (InterruptedException e) {}

                    p.recordLock.lock();
                    p.invalidateServers(invalidServerIds);
                    p.recordLock.unlock();
                    p.serverLock.unlock();
                    System.out.println("*** CHECKPOINT END ***");
                }
            }
            ,0, this.config.getLong("CheckpointFreq"), TimeUnit.MILLISECONDS);
    }

    @Override
    /* Registers a server with our server. Once communication with client starts, no further servers should be
       registered
     */
    protected void serverRegistered(long id, BankAccountStub stub)
    {
        this.serverLock.lock();
        try{
            if (this.servers.isEmpty()){
                stub.setPrimary();
                this.primaryServerId = id;
            } else {
                stub.setBackup();
            }
        } catch (BankAccountStub.NoConnectionException e){
            System.out.printf("Server %d failed to register due to NoConnectionException%n", id);
            this.serverLock.unlock();
            return;
        } catch (IncorrectOperation incorrectOperation) {
            // TODO: handle this?
            incorrectOperation.printStackTrace();
        }
        this.servers.put(id, stub);
        this.serverLock.unlock();
    }

    private void sendToAllServers(final Message message){
        this.serverLock.lock();

        // if no servers to send to, simply send the exception and undo
        if (this.servers.isEmpty()){
            this.clientProxy.RequestUnsuccessfulException(message.reqid);
            this.serverLock.unlock();
            return;
        }

        // add to message record before we start sending out messages, so threads can respond to the message
        // as soon as possible without waiting for us to invalidate servers.
        PendingMessageRecord rec = new PendingMessageRecord(message, new HashSet<>(this.servers.keySet()));
        this.recordLock.lock();
        this.pendingMessageRecords.put(message.reqid, rec);
        this.recordLock.unlock();

        // Create a new thread pool to handle each request
        ExecutorService executorService = Executors.newFixedThreadPool(this.servers.keySet().size());
        final HashMap<Long, BankAccountStub> servers = this.servers;
        // Generate a list of tasks that return either a failed serverid or null
        Collection<Callable<Long>> tasks = new ArrayList<>();
        for (final Long serverid : this.servers.keySet()) {
            Callable<Long> task = new Callable<Long>() {
                public Long call() {
                    System.out.printf("Attempting to send message %d to server %d%n", message.reqid, serverid);
                    BankAccountStub stub = servers.get(serverid);
                    assert(stub != null); //TODO: remove this
                    try{
                        if (message.type == MessageType.UPDATE) {
                            stub.beginChangeBalance(message.reqid, message.value);
                        } else{
                            stub.beginReadBalance(message.reqid);
                        }
                    } catch (BankAccountStub.NoConnectionException e){
                        System.out.printf("Server %d threw NoConnectionException%n", serverid);
                        return serverid;
                    }
                    System.out.printf("Message %d sent successfully to server %d %n", message.reqid, serverid);
                    return null;
                }
            };
            tasks.add(task);
        }

        // Collect all invalidServerIds from the result of the executor
        HashSet<Long> invalidServerIds = new HashSet<>();
        List<Future<Long>> results;
        try {
            results = executorService.invokeAll(tasks);
            for (Future<Long> result : results){
                Long l = null;
                try { l = result.get(); } catch (ExecutionException e){}
                if (l != null){
                    invalidServerIds.add(l);
                }
            }
        } catch (InterruptedException e) {}

        this.recordLock.lock();
        this.invalidateServers(invalidServerIds);
        this.recordLock.unlock();
        this.serverLock.unlock();
    }

    private void electNewPrimary(){
        HashSet<Long> invalidServerIds = new HashSet<>();
        this.primaryServerId = -1;
        for (final Long serverid : this.servers.keySet()) {
            BankAccountStub stub = this.servers.get(serverid);
            try{
                stub.setPrimary();
            } catch (BankAccountStub.NoConnectionException e) {
                invalidServerIds.add(serverid);
                continue;
            } catch (IncorrectOperation incorrectOperation) {
                incorrectOperation.printStackTrace();
                //TODO: handle this
            }
            this.primaryServerId = serverid;
            break;
        }
        invalidateServers(invalidServerIds);
    }

    /* Purges all records of a serverid from the data structures
       REQUIRES serverLock, recordLock TO BE LOCKED ALREADY BEFORE CALLING
       IF CALLED FROM QUIESCENCE MODE: all messages have been handled
       ELSE: purge records containing the serverid
     */
    private void invalidateServers(Collection<Long> serverids){
        // remove each server id
        for (Long serverid : serverids){
            // only invalidate if server has not been invalidated already
            if (this.servers.containsKey(serverid)){
                System.out.printf("Invalidating server %d%n", serverid);
                this.servers.remove(serverid);
                for (PendingMessageRecord rec : this.pendingMessageRecords.values()){
                    if (rec.reportLostMessage(serverid)){
                        // lost last message, and no response from anyone, so throw error
                        if (!rec.suppress){
                            this.clientProxy.RequestUnsuccessfulException(rec.message.reqid);
                        }
                        this.pendingMessageRecords.remove(rec.message.reqid);
                    }
                }
            }
        }
        // elect new primary server if needed
        if (serverids.contains(this.primaryServerId)){
            this.electNewPrimary();
        }
    }

    // called by Client
    @Override
    protected void beginReadBalance(int reqid)
    {
        System.out.printf("Received a read balance message with id %d%n", reqid);
        final Message m = new Message(MessageType.READ, reqid);
        Thread t = new Thread(new Runnable(){
            public void run(){
                sendToAllServers(m);
            }
        });
        t.start();
        System.out.printf("Forked a thread with pid %d to handle request %d%n", t.getId(), reqid);
    }
    // called by Client
    @Override
    protected void beginChangeBalance(int reqid, int update)
    {
        System.out.printf("Received a change balance message with id %d%n", reqid);
        final Message m = new Message(MessageType.UPDATE, reqid, update);
        Thread t = new Thread(new Runnable(){
            public void run(){
                sendToAllServers(m);
            }
        });
        t.start();
        System.out.printf("Forked a thread with pid %d to handle request %d%n", t.getId(), reqid);
    }

    private void endBalanceHelper(long serverid, int reqid, int balance, MessageType messageType){
        this.recordLock.lock();
        PendingMessageRecord rec = this.pendingMessageRecords.get(reqid);
        assert(rec != null); //TODO: remove this
        // if first message to go out, send return value and suppress further messages
        if (!rec.suppress){
            rec.suppress = true;
            if (messageType == MessageType.UPDATE){
                this.clientProxy.endChangeBalance(reqid, balance);
            } else {
                this.clientProxy.endReadBalance(reqid, balance);
            }
        }
        // report reception to the pending message pool
        if (rec.reportReception(serverid)){
            pendingMessageRecords.remove(reqid);
        }
        this.recordLock.unlock();
    }

    // called by Servers
    @Override
    protected void endReadBalance(long serverid, int reqid, int balance)
    {
        System.out.printf("Recieved read balance=%d from server %d for request %d%n", balance, serverid, reqid);
        endBalanceHelper(serverid, reqid, balance, MessageType.READ);
    }
    // called by Servers
    @Override
    protected void endChangeBalance(long serverid, int reqid, int balance)
    {
        System.out.printf("Recieved change balance=%d from server %d for request %d%n", balance, serverid, reqid);
        endBalanceHelper(serverid, reqid, balance, MessageType.UPDATE);
    }

    @Override
    protected void serversFailed(List<Long> failedServers)
    {
        System.out.printf("Server heartbeat missed... invalidating%n");
        super.serversFailed(failedServers);
        this.serverLock.lock();
        this.recordLock.lock();
        invalidateServers(failedServers);
        this.recordLock.unlock();
        this.serverLock.unlock();
    }
}
