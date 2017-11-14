package edu.cmu.rds749.lab3;

import edu.cmu.rds749.common.AbstractServer;
import org.apache.commons.configuration2.Configuration;
import rds749.Checkpoint;

import edu.cmu.rds749.lab3.Proxy.MessageType;
import edu.cmu.rds749.lab3.Proxy.Message;

import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;
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

    private int balance = 0;
    private ProxyControl ctl;
    private Checkpoint checkpoint = null;

    private ServerType serverType;
    private Queue<Message> messageQueue;
    private int lastReqid;

    private Lock lock;

    public BankAccountI(Configuration config) {
        super(config);
        /* By default, a new bank account should be declared as BACKUP, so if messages come in before the SetPrimary or
         * SetBackup is called, the messages will not get lost */
        this.serverType = ServerType.BACKUP;
        this.messageQueue = new LinkedList<>();
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
        Message m;
        do{
            m = this.messageQueue.poll(); // pop oldest message
        } while(m != null || m.reqid < checkpoint.reqid);
        // remove all messages with reqids <= the checkpoints reqid
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
        Message m;
        // go through all messages in message queue and process them as the primary
        while (true){
            m = this.messageQueue.poll(); // pop oldest message
            if (m == null){
                break;
            }
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