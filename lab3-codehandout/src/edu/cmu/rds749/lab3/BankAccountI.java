package edu.cmu.rds749.lab3;

import edu.cmu.rds749.common.AbstractServer;
import org.apache.commons.configuration2.Configuration;
import rds749.Checkpoint;

import java.util.PriorityQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implements the BankAccounts transactions interface
 * Created by utsav on 8/27/16.
 */

public class BankAccountI extends AbstractServer
{
    public enum ServerType {
        PRIMARY, BACKUP
    }
    public enum MessageType{
        READ, UPDATE
    }

    private class Message implements Comparable<Message>{
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

    private int balance = 0;
    private ProxyControl ctl;
    private Checkpoint checkpoint = null;

    private ServerType serverType;
    private PriorityQueue<Message> messageQueue;
    private int lastReqid;

    private Lock lock;

    public BankAccountI(Configuration config) {
        super(config);
        /* By default, a new bank account should be declared as BACKUP, so if messages come in before the SetPrimary or
         * SetBackup is called, the messages will not get lost */
        this.serverType = ServerType.BACKUP;
        this.messageQueue = new PriorityQueue<>();
        /* lastReqId allows us to create a checkpoint, providing the checkpoint with the most recently consumed message
         * so backups know how to prune their logs */
        this.lastReqid = -1;
        this.lock = new ReentrantLock();
    }

    @Override
    protected void doStart(ProxyControl ctl) throws Exception {
        this.ctl = ctl;
    }

    private void handleMessage(Message m){
        this.lastReqid = m.reqid;
        if (this.serverType == ServerType.PRIMARY) {
            if (m.type == MessageType.READ) {
                this.ctl.endReadBalance(m.reqid, this.balance);
            } else {
                this.balance += m.value;
                this.ctl.endChangeBalance(m.reqid, this.balance);
            }
        } else {
            this.messageQueue.add(m);
        }
    }

    @Override
    protected void handleBeginReadBalance(int reqid)
    {
        Message m = new Message(MessageType.READ, reqid);
        this.lock.lock();
        handleMessage(m);
        this.lock.unlock();
    }

    @Override
    protected void handleBeginChangeBalance(int reqid, int update)
    {
        Message m = new Message(MessageType.UPDATE, reqid, update);
        this.lock.lock();
        handleMessage(m);
        this.lock.unlock();
    }

    @Override
    protected Checkpoint handleGetState()
    {
        this.lock.lock();
        assert(this.serverType == ServerType.PRIMARY);
        assert(this.messageQueue.isEmpty());
        Checkpoint checkpoint = new Checkpoint(this.lastReqid, this.balance);
        this.lock.unlock();
        return checkpoint;
    }

    @Override
    protected int handleSetState(Checkpoint checkpoint)
    {
        this.lock.lock();
        Message m = null;
        // remove all messages with reqids <= the checkpoints reqid
        while (m != null || m.reqid < checkpoint.reqid){
            m = this.messageQueue.poll(); // pop oldest message
        }
        assert(m.reqid == checkpoint.reqid);
        this.balance = checkpoint.state;
        this.lock.unlock();
        return this.balance;
    }

    @Override
    protected void handleSetPrimary()
    {
        this.lock.lock();
        this.serverType = ServerType.PRIMARY;
        Message m = null;
        // go through all messages in message queue and process them as the primary
        while (m != null){
            m = this.messageQueue.poll(); // pop oldest message
            this.handleMessage(m); // process message
        }
        this.lock.unlock();
    }

    @Override
    protected void handleSetBackup()
    {
        this.lock.lock();
        this.serverType = ServerType.BACKUP;
        this.lock.unlock();
    }
}