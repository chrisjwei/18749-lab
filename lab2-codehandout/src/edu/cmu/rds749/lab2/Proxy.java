package edu.cmu.rds749.lab2;

import edu.cmu.rds749.common.AbstractProxy;
import edu.cmu.rds749.common.BankAccountStub;
import org.apache.commons.configuration2.Configuration;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;

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
    private ConcurrentHashMap<Long, BankAccountStub> servers;

    public Proxy(Configuration config)
    {
        super(config);
        this.messageQueue = new ConcurrentLinkedQueue<>();
        this.pendingMessageRecords = new ConcurrentHashMap<>();
        this.servers = new ConcurrentHashMap<>();
    }

    @Override
    protected void serverRegistered(long id, BankAccountStub stub)
    {
        
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

    @Override
    protected void serversFailed(List<Long> failedServers)
    {
        super.serversFailed(failedServers);
    }
}
