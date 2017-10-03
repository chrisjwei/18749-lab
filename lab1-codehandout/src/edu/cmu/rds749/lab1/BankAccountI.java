package edu.cmu.rds749.lab1;

import edu.cmu.rds749.common.AbstractServer;
import org.apache.commons.configuration2.Configuration;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Implements the BankAccounts transactions interface
 * Created by utsav on 8/27/16.
 */

public class BankAccountI extends AbstractServer
{
    private final Configuration config;

    private int balance = 0;
    private long serverId = -1;
    private ProxyControl ctl; // TODO: how is doStart ever called?

    public BankAccountI(Configuration config) {
        super(config);
        this.config = config;
    }

    protected void doStart(ProxyControl ctl) throws Exception {
        this.serverId = ctl.register(this.config.getString("serverHost"), this.config.getInt("serverPort"));
        //TODO: fork thread for heartbeats
        //TODO: start heartbeat with server
    }

    protected int handleReadBalance() {
        return this.balance;
    }

    protected int handleChangeBalance(int update) {
        this.balance += update;
        return this.balance;
    }

}
