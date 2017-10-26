package edu.cmu.rds749.lab2;

import edu.cmu.rds749.common.AbstractServer;
import org.apache.commons.configuration2.Configuration;

/**
 * Implements the BankAccounts transactions interface
 */

public class BankAccountI extends AbstractServer
{
    private final Configuration config;

    private int balance = 0;
    private ProxyControl ctl;

    //TODO: do we need synchronization?

    public BankAccountI(Configuration config) {
        super(config);
        this.config = config;
    }

    @Override
    protected void doStart(ProxyControl ctl) throws Exception {
        this.ctl = ctl;
    }

    // called by Proxy
    @Override
    protected void handleBeginReadBalance(int reqid) {
        final int reqid_f = reqid;
        final int balance_f = this.balance;
        final ProxyControl ctl_f = this.ctl;
        System.out.printf("Received a read balance message with id %d%n", reqid);
        Thread t = new Thread(new Runnable(){
            public void run(){
                ctl_f.endReadBalance(reqid_f, balance_f);
                System.out.printf("Sent a read balance message with id %d%n", reqid_f);
            }
        });
        t.start();
        System.out.printf("Forked a thread with pid %d to handle message &d%n", t.getId(), reqid);
    }
    // called by Proxy
    @Override
    protected void handleBeginChangeBalance(int reqid, int update) {
        this.balance += update;

        final int reqid_f = reqid;
        final int balance_f = this.balance;
        final ProxyControl ctl_f = this.ctl;
        System.out.printf("Received a change balance message with id %d%n", reqid);
        Thread t = new Thread(new Runnable(){
            public void run(){
                ctl_f.endChangeBalance(reqid_f, balance_f);
                System.out.printf("Sent a change balance message with id %d%n", reqid_f);
            }
        });
        t.start();
        System.out.printf("Forked a thread with pid %d to handle message &d%n", t.getId(), reqid);
    }

    @Override
    protected int handleGetState(){
        return this.balance;
    }

    @Override
    protected int handleSetState(int balance){
        this.balance = balance;
        return balance;
    }
}
