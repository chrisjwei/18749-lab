package edu.cmu.rds749.lab2;

import edu.cmu.rds749.common.AbstractProxy;
import edu.cmu.rds749.common.BankAccountStub;
import org.apache.commons.configuration2.Configuration;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/**
 *
 * Implements the Proxy.
 */
public class Proxy extends AbstractProxy
{
    public class Message {

        public final static int READBALANCE = 0;
        public final static int CHANGEBALANCE = 1;

        public int messageType;
        public int value;
        public int reqid;

        public Message(int messageType, int reqid){
            this.messageType = messageType;
            this.reqid = reqid;
        }

        public Message(int messageType, int reqid, int value){
            this.messageType = messageType;
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

    public class QuiescenseLock{

        int inflightMessageCount;
        int quiescenceCount;
        Lock lock;
        Condition quiescenceCond;
        Condition inflightCond;


        public QuiescenseLock(){
            this.inflightMessageCount = 0;
            this.quiescenceCount = 0;
            this.lock = new ReentrantLock();
            this.quiescenceCond = lock.newCondition();
            this.inflightCond = lock.newCondition();
        }

        /* Declare that there is a message that is being serviced */
        public void requestBusy(){
            this.lock.lock();
            // block until all membership changes are finished
            while (this.quiescenceCount != 0){
                this.quiescenceCond.awaitUninterruptibly();
            }
            this.inflightMessageCount += 1;
            this.lock.unlock();
        }
        /* Declare that all copies of a message has been either serviced or failed */
        public void releaseBusy(){
            this.lock.lock();
            this.inflightMessageCount -= 1;
            if (this.inflightMessageCount == 0){
                this.inflightCond.signalAll();
            }
            this.lock.unlock();
        }
        /* Declare that we want all incoming messages to wait until we finish bringing a server up to speed */
        public void requestQuiescense(){
            this.lock.lock();
            // change quiescense count so future messages are blocked
            this.quiescenceCount += 1;
            // wait until all inflight messages are serviced
            while (this.inflightMessageCount != 0){
                this.inflightCond.awaitUninterruptibly();
            }
            this.lock.unlock();
        }
        /* Declare that we have brought a server up to speed and messages can be continued to be served */
        public void releaseQuiescense(){
            this.lock.lock();
            this.quiescenceCount -= 1;
            if (this.quiescenceCount == 0){
                this.quiescenceCond.signalAll();
            }
            this.lock.unlock();
        }
    }

    private Lock recordLock;
    private HashMap<Integer,PendingMessageRecord> pendingMessageRecords;

    private Lock serverLock;
    private HashMap<Long, BankAccountStub> servers;

    private QuiescenseLock quiescenseLock;

    public Proxy(Configuration config)
    {
        super(config);
        this.recordLock = new ReentrantLock();
        this.pendingMessageRecords = new HashMap<>();
        this.serverLock = new ReentrantLock();
        this.servers = new HashMap<>();
        this.quiescenseLock = new QuiescenseLock();
    }

    @Override
    protected void serverRegistered(long id, BankAccountStub stub)
    {
        // declare that we need quiescense
        this.quiescenseLock.requestQuiescense();
        this.serverLock.lock();

        // default state
        int state = 0;
        Set<Long> invalidServerIds = new HashSet<>();
        for (long serverid : this.servers.keySet()){
            BankAccountStub selectedStub = this.servers.get(serverid);
            assert(selectedStub != null); //TODO: remove this
            try {
                state = selectedStub.getState();
            } catch (BankAccountStub.NoConnectionException e){
                invalidServerIds.add(serverid);
                continue;
            }
            break;
        }
        // Remove servers that failed to respond
        if (!invalidServerIds.isEmpty()){
            this.recordLock.lock();
            this.invalidateServers(invalidServerIds);
            this.recordLock.unlock();
        }
        // if we found at least one working server with the state, update the new server with it
        try {
            stub.setState(state);
        } catch (BankAccountStub.NoConnectionException e){
            // server died before we could add it
            this.serverLock.unlock();
            this.quiescenseLock.releaseQuiescense();
            return;
        }
        // either first server or successfully found and updated with new state
        this.servers.put(id, stub);
        this.serverLock.unlock();
        this.quiescenseLock.releaseQuiescense();
    }

    // called by Client
    @Override
    protected void beginReadBalance(int reqid)
    {
        System.out.printf("Received a read balance message with id %d%n", reqid);
        final Message m = new Message(Message.READBALANCE, reqid);
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
        final Message m = new Message(Message.CHANGEBALANCE, reqid, update);
        Thread t = new Thread(new Runnable(){
            public void run(){
                sendToAllServers(m);
            }
        });
        t.start();
        System.out.printf("Forked a thread with pid %d to handle request %d%n", t.getId(), reqid);
    }

    private void endBalanceHelper(long serverid, int reqid, int balance, int messageType){
        this.recordLock.lock();
        PendingMessageRecord rec = this.pendingMessageRecords.get(reqid);
        assert(rec != null); //TODO: remove this
        // if first message to go out, send return value and suppress further messages
        if (!rec.suppress){
            rec.suppress = true;
            if (messageType == Message.CHANGEBALANCE){
                this.clientProxy.endChangeBalance(reqid, balance);
            } else {
                this.clientProxy.endReadBalance(reqid, balance);
            }
        }
        // report reception to the pending message pool
        if (rec.reportReception(serverid)){
            pendingMessageRecords.remove(reqid);
            this.quiescenseLock.releaseBusy(); // declare that all messages have been processed
        }
        this.recordLock.unlock();
    }

    // called by Servers
    @Override
    protected void endReadBalance(long serverid, int reqid, int balance)
    {
        System.out.printf("Recieved read balance=%d from server %d for request %d%n", balance, serverid, reqid);
        endBalanceHelper(serverid, reqid, balance, Message.READBALANCE);
    }
    // called by Servers
    @Override
    protected void endChangeBalance(long serverid, int reqid, int balance)
    {
        System.out.printf("Recieved change balance=%d from server %d for request %d%n", balance, serverid, reqid);
        endBalanceHelper(serverid, reqid, balance, Message.CHANGEBALANCE);
    }

    private void sendToAllServers(final Message message){
        // block until all membership changes finish
        this.quiescenseLock.requestBusy();
        this.serverLock.lock();

        // if no servers to send to, simply send the exception and undo
        if (this.servers.isEmpty()){
            this.clientProxy.RequestUnsuccessfulException(message.reqid);
            this.serverLock.unlock();
            this.quiescenseLock.releaseBusy();
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
                        if (message.messageType == Message.CHANGEBALANCE) {
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

    /* Purges all records of a serverid from the data structures
       REQUIRES serverLock, recordLock TO BE LOCKED ALREADY BEFORE CALLING
       IF CALLED FROM QUIESCENCE MODE: all messages have been handled
       ELSE: purge records containing the serverid
     */
    private void invalidateServers(Collection<Long> serverids){
        // remove each server id
        for (Long serverid : serverids){
            // only invalidate if server has not been invalidated already
            if (servers.containsKey(serverid)){
                System.out.printf("Invalidating server %d%n", serverid);
                servers.remove(serverid);
                // only happen in non-quiescent mode
                for (PendingMessageRecord rec : pendingMessageRecords.values()){
                    if (rec.reportLostMessage(serverid)){
                        // lost last message, and no response from anyone, so throw error
                        if (!rec.suppress){
                            this.clientProxy.RequestUnsuccessfulException(rec.message.reqid);
                        }
                        pendingMessageRecords.remove(rec.message.reqid);
                        this.quiescenseLock.releaseBusy(); // declare that all messages have been processed
                    }
                }
            }
        }
    }

    @Override
    protected void serversFailed(List<Long> failedServers)
    {
        System.out.printf("Server heartbeat missed... invalidating%n");
        super.serversFailed(failedServers);
        this.serverLock.lock();
        invalidateServers(failedServers);
        this.serverLock.unlock();
    }
}
