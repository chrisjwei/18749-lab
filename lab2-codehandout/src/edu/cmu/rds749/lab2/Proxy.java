package edu.cmu.rds749.lab2;

import edu.cmu.rds749.common.AbstractProxy;
import edu.cmu.rds749.common.BankAccountStub;
import org.apache.commons.configuration2.Configuration;

import java.util.*;
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
        public int returnValue;

        public PendingMessageRecord(Message message, Set<Long> serverids){
            this.serverids = serverids;
            this.message = message;
            this.suppress = false;
            this.returnValue = 0;
        }

        public void reportReception(long serverid){
            boolean removed = this.serverids.remove(serverid);
            assert(removed); // TODO: remove this
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

        Iterator<Long> serverids = this.servers.keySet().iterator();
        int state = 0;
        boolean foundState = false;
        while(serverids.hasNext()){
            long selectedId = serverids.next();
            BankAccountStub selectedStub = this.servers.get(selectedId);
            try {
                state = selectedStub.getState();
            } catch (BankAccountStub.NoConnectionException e){
                this.recordLock.lock();
                this.invalidateServer(selectedId);
                this.recordLock.unlock();
                continue;
            }
            foundState = true;
            break;
        }
        // if we found at least one working server with the state, update the new server with it
        if (foundState){
            try {
                stub.setState(state);
            } catch (BankAccountStub.NoConnectionException e){
                // server died before we could add it
                this.serverLock.unlock();
                this.quiescenseLock.releaseQuiescense();
                return;
            }
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
        System.out.println("(In Proxy)");
    }
    // called by Client
    @Override
    protected void beginChangeBalance(int reqid, int update)
    {
        System.out.println("(In Proxy)");
    }
    // called by Servers
    @Override
    protected void endReadBalance(long serverid, int reqid, int balance)
    {
        System.out.println("(In Proxy)");
    }
    // called by Servers
    @Override
    protected void endChangeBalance(long serverid, int reqid, int balance)
    {
        System.out.println("(In Proxy)");
    }

    private void sendToAllServers(Message message){
        // block until all membership changes finish
        this.quiescenseLock.requestBusy();
        this.serverLock.lock();

        Set<Long> validServerIds = new HashSet<>();
        for (Long serverid : this.servers.keySet()){
            BankAccountStub stub = this.servers.get(serverid);
            if (message.messageType == Message.CHANGEBALANCE){
                try{
                    stub.beginChangeBalance(message.reqid, message.value);
                } catch (BankAccountStub.NoConnectionException e){
                    this.recordLock.lock();
                    this.invalidateServer(serverid);
                    this.recordLock.unlock();
                    continue;
                }
                validServerIds.add(serverid);
            }
        }
        this.recordLock.lock();
        this.pendingMessageRecords.put(message.reqid, new PendingMessageRecord(message, validServerIds));
        this.recordLock.unlock();
        this.serverLock.unlock();
    }

    /* Purges all records of a serverid from the data structures
       REQUIRES serverLock, recordLock TO BE LOCKED ALREADY BEFORE CALLING
       IF CALLED FROM QUIESCENCE MODE: all messages have been handled
       ELSE: purge records containing the serverid
     */
    private void invalidateServer(long serverid){
        List l = new ArrayList<Long>();
        l.add(serverid);
        serversFailed(l);

        servers.remove(serverid);
        // only happen in non-quiescent mode
        for (PendingMessageRecord rec : pendingMessageRecords.values()){
            rec.serverids.remove(serverid);
            if (rec.serverids.isEmpty()) {
                if (!rec.suppress){
                    // TODO: call RequestUnsuccessfulException
                }
                pendingMessageRecords.remove(rec);
                // THIS SHOULD NOT BLOCK
                this.quiescenseLock.releaseBusy(); // declare that all messages have been processed
            }
        }
    }

    @Override
    protected void serversFailed(List<Long> failedServers)
    {
        super.serversFailed(failedServers);
    }
}
