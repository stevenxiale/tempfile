package com.jd.orderpat.client;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

/**
 * Created by xiale on 14-8-4.
 */
public class VoltDbJob implements Runnable{

    private long sumLong;
    public long getSumLong() {
        return sumLong;
    }

    public void setSumLong(long sumLong) {
        this.sumLong = sumLong;
    }

    CountDownLatch countdown = null;
    private String connString = null;
    public VoltDbJob(CountDownLatch countdown,String connString)
    {
        this.countdown = countdown;
        this.connString = connString;
    }
    


	public void run(){
        long dStart=System.currentTimeMillis();
        
        //this.callCountStoreProduce(connString);
        //callJoinStoreProduce(connString);
        callStoreProduce(connString);
        long dLong = System.currentTimeMillis()-dStart;
        sumLong += dLong;
        System.out.println("current thread:"+Thread.currentThread().getId() + " voltDb执行耗时 : "+dLong+" 毫秒 ");
        this.countdown.countDown();
    }
	
	private void callCountStoreProduce(String connString) {
        
        ClientConfig clientConfig = new ClientConfig("program", "none");
        org.voltdb.client.Client client =
            org.voltdb.client.ClientFactory.createClient(clientConfig);
        
        // Client instance connected to the database running on
        // the specified IP address, in this case 127.0.0.1. The
        // database always runs on TCP/IP port 21212.
        try {
            client.createConnection("Hadoop-sandbox-08.pekdc1.jdfin.local");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        // Example of executing a dynamic SQL query
        String tableName = "TOWNS";
        //VoltTable[] count = client.callProcedure("@AdHoc", "SELECT COUNT(*) FROM " + tableName).getResults();
        //System.out.printf("Found %d employees.\n", count[0].fetchRow(0).getLong(0));
        
        
        // Synchronous call to find out what department Larry Merchant works in
        ClientResponse response = null;
        try {
            response = client.callProcedure(connString);
        } catch (NoConnectionsException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ProcCallException e) {
            e.printStackTrace();
        }
        if (response.getStatus() != ClientResponse.SUCCESS) {
            System.err.println(response.getStatusString());
            System.exit(-1);
        }

        final VoltTable results[] = response.getResults();
       
        VoltTable result = results[0];
        VoltTableRow row = result.fetchRow(0);
        System.out.printf("------ Total recourds is %d\n", row.getLong(0));
	}
	
    private static void callJoinStoreProduce(String connString) {
        
        ClientConfig clientConfig = new ClientConfig("program", "none");
        org.voltdb.client.Client client =
            org.voltdb.client.ClientFactory.createClient(clientConfig);
        
        // Client instance connected to the database running on
        // the specified IP address, in this case 127.0.0.1. The
        // database always runs on TCP/IP port 21212.
        try {
            client.createConnection("Hadoop-sandbox-08.pekdc1.jdfin.local");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        ClientResponse response = null;
        try {
            response = client.callProcedure(connString);
        } catch (NoConnectionsException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ProcCallException e) {
            e.printStackTrace();
        }
        if (response.getStatus() != ClientResponse.SUCCESS) {
            System.err.println(response.getStatusString());
            System.exit(-1);
        }

        final VoltTable results[] = response.getResults();
       
        VoltTable result = results[0];
        VoltTableRow row = result.fetchRow(0);
        System.out.printf("------ one recourd is %s\n", row.getString("town"));
    }
    
    private static void callStoreProduce(String produceName) {
        System.out.println("Client started");

        ClientConfig clientConfig = new ClientConfig("program", "none");
        org.voltdb.client.Client client = org.voltdb.client.ClientFactory
                .createClient(clientConfig);

        // Client instance connected to the database running on
        // the specified IP address, in this case 127.0.0.1. The
        // database always runs on TCP/IP port 21212.
        try {
            client.createConnection("Hadoop-sandbox-08.pekdc1.jdfin.local");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        ClientResponse response = null;
        try {
            response = client.callProcedure(produceName);
        } catch (NoConnectionsException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ProcCallException e) {
            e.printStackTrace();
        }
        if (response.getStatus() != ClientResponse.SUCCESS) {
            System.err.println(response.getStatusString());
            System.exit(-1);
        }

        System.out.printf(produceName + " ended");
    }
}
