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
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public BankAccountI(Configuration config) {
        super(config);
        this.config = config;
    }

    protected void doStart(final ProxyControl ctl) throws Exception {
        final long id = ctl.register(this.config.getString("serverHost"), this.config.getInt("serverPort"));
        this.serverId = id;
        this.scheduler.scheduleAtFixedRate(new Runnable(){
            public void run(){
                System.out.printf("heartbeating proxy from serverId=%d...%n", id);
                try{
                    ctl.heartbeat(id, System.currentTimeMillis());
                } catch (IOException e){
                    System.out.println("IO exception raised.");
                    //TODO: should we retry right away or just try at next interval?
                }
            }
        },0, this.config.getLong("heartbeatIntervalMillis"), TimeUnit.MILLISECONDS);
    }

    protected int handleReadBalance() {
        return this.balance;
    }

    protected int handleChangeBalance(int update) {
        this.balance += update;
        return this.balance;
    }

}
