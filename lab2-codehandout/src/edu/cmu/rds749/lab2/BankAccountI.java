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
        this.ctl.endReadBalance(reqid, this.balance);
    }
    // called by Proxy
    @Override
    protected void handleBeginChangeBalance(int reqid, int update) {
        this.balance += update;
        this.ctl.endChangeBalance(reqid, this.balance);
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
