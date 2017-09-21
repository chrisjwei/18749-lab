package edu.cmu.rds749.lab0;

import Ice.Current;
import edu.cmu.rds749.common.AbstractProxy;

/**
 * Created by jiaqi on 8/28/16.
 *
 * Chris Wei    (cjwei)
 * Mark Horvath (mhorvath)
 *
 * Implements the Proxy.
 */
public class Proxy extends AbstractProxy
{
    public Proxy(String [] hostname, String [] port)
    {
        super(hostname, port);
    }

    public int readBalance(Current __current)
    {
        System.out.println("(In Proxy)");
        return this.actualBankAccounts[0].readBalance();
    }

    public int changeBalance(int update, Current __current)
    {
        System.out.println("(In Proxy)");
        return this.actualBankAccounts[0].changeBalance(update);
    }

}
