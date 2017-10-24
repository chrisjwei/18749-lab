package edu.cmu.rds749.lab2;

import edu.cmu.rds749.common.AbstractProxy;
import edu.cmu.rds749.common.BankAccountStub;
import org.apache.commons.configuration2.Configuration;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * Implements the Proxy.
 */
public class Proxy extends AbstractProxy
{
    public class Message {

        public final int READBALANCE = 0;
        public final int CHANGEBALANCE = 1;

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
        public int messageid;
        public boolean suppress;

        public PendingMessageRecord(int messageid, Set<Long> serverids){
            this.serverids = serverids;
            this.messageid = messageid;
            this.suppress = false;
        }

        public void reportReception(long serverid){
            boolean removed = this.serverids.remove(serverid);
            assert(removed); // TODO: remove this
        }
    }

    private ConcurrentLinkedQueue<Message> messageQueue;
    private ConcurrentHashMap<Integer,PendingMessageRecord> pendingMessageRecords;
    private Lock quiescenceLock;
    private Condition quiescenceCondition;
    private ConcurrentHashMap<Long, BankAccountStub> servers;

    public Proxy(Configuration config)
    {
        super(config);
        this.messageQueue = new ConcurrentLinkedQueue<>();
        this.pendingMessageRecords = new ConcurrentHashMap<>();
        this.servers = new ConcurrentHashMap<>();
        this.quiescenceLock = new ReentrantLock();
        this.quiescenceCondition = this.quiescenceLock.newCondition();
    }

    @Override
    protected void serverRegistered(long id, BankAccountStub stub)
    {
        BankAccountStub existingStub;
        Long existingId;
        Enumeration<BankAccountStub> stubEnum;
        Enumeration<Long> idEnum;
        int state;
        if (servers.isEmpty()){
            servers.put(id, stub);
            return;
        }
        this.quiescenceLock.lock();
        try {
            while (!pendingMessageRecords.isEmpty()) {
                this.quiescenceCondition.await();
            }
            stubEnum = servers.elements();
            idEnum = servers.keys();
            try {
                while(true){
                    existingStub = stubEnum.nextElement();
                    existingId = idEnum.nextElement();
                    try {
                        state = existingStub.getState();
                    } catch (BankAccountStub.NoConnectionException e){
                        invalidateServer(existingId);
                        continue;
                    }
                    break;
                }
                try {
                    stub.setState(state);
                } catch (BankAccountStub.NoConnectionException e){
                    this.quiescenceLock.unlock();
                    return;
                }
                servers.put(id, stub);
            } catch (NoSuchElementException e){
                // all other servers died before quiescence was achieved
                servers.put(id, stub);
            }
        } catch (InterruptedException e){
            // TODO: probably don't need to do anything here
        } finally{
            this.quiescenceLock.unlock();
        }
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
        return; // TODO
    }

    private void invalidateServer(long serverid){
        List l = new ArrayList<Long>();
        boolean removedAtleastOneRec = false;
        l.add(serverid);
        serversFailed(l);
        servers.remove(serverid);
        for (PendingMessageRecord rec : pendingMessageRecords.values()){
            // only happen in non-quiescent mode
            rec.serverids.remove(serverid);
            if (rec.serverids.isEmpty()) {
                if (!rec.suppress){
                    // tell client that all servers ahve failed
                }
                pendingMessageRecords.remove(rec);
                removedAtleastOneRec = true;
            }
        }
        if (pendingMessageRecords.isEmpty() && removedAtleastOneRec){
            quiescenceLock.lock();
            quiescenceCondition.signal();
            quiescenceLock.unlock();
        }
    }

    @Override
    protected void serversFailed(List<Long> failedServers)
    {
        super.serversFailed(failedServers);
    }
}
