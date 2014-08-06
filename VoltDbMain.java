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
 * Created by xiale on 14-8-3.
 */
public class VoltDbMain {

    private static final String queryProceducevoltDb = "QueryTowns";
    private static final String joinProceducevoltDb = "JoinTowns";

    public static void main(String[] args) {

        // SingleRun();

        // ThreadRun();
        if (args.length < 2) {
            System.out.println("Please input at least 2 args");
            return;
        }
        if ("single".equals(args[0])) {
            SingleRun(args[1]);
        } else if ("multi".equals(args[0])) {
            if (args.length < 3) {
                System.out
                        .println("Please input at least 3 args when call multithreading");
                return;
            }
            ThreadRun(args[1], Integer.parseInt(args[2]));
        }

        // System.out.println("sdfasdf:"+ Math.pow(1.95,(1.0/3)));

    }

    private static void SingleRun(String produceName) {
        runVoltDb(produceName);
    }

    // 单次执行sql
    private static void SingleRun() {
        runVoltDb();
    }

    // 线程并发执行sql
    private static int threadCount = 50;

    private static void ThreadRun() {
        long dStart = System.currentTimeMillis();
        int threadCountVoltDb = threadCount;
        CountDownLatch countdownVoltDb = new CountDownLatch(threadCountVoltDb);
        // HadoopJob hadoopJob = new HadoopJob(countdownHadoop,connHadoop);
        VoltDbJob voltDbJob = new VoltDbJob(countdownVoltDb,
                queryProceducevoltDb);
        for (int i = 0; i < threadCountVoltDb; i++) {
            Thread t = new Thread(voltDbJob);
            t.start();
        }
        try {
            countdownVoltDb.await();
        } catch (InterruptedException e) {
            System.out.println("VoltDb线程并行失败...");
            e.printStackTrace();
        }

        System.out.println(voltDbJob.getSumLong());
        long dLong = System.currentTimeMillis() - dStart;
        System.out.println("voltDb访问线程数【" + threadCountVoltDb + "】，总执行耗时: "
                + dLong + " 毫秒 ");
        System.out.println("voltDb访问线程数【" + threadCountVoltDb + "】，平均执行耗时: "
                + voltDbJob.getSumLong() / threadCountVoltDb + " 毫秒 ");

    }

    private static void ThreadRun(String produceName, int count) {
        long dStart = System.currentTimeMillis();
        int threadCountVoltDb = threadCount;
        CountDownLatch countdownVoltDb = new CountDownLatch(threadCountVoltDb);
        // HadoopJob hadoopJob = new HadoopJob(countdownHadoop,connHadoop);
        VoltDbJob voltDbJob = new VoltDbJob(countdownVoltDb, produceName);
        for (int i = 0; i < count; i++) {
            Thread t = new Thread(voltDbJob);
            t.start();
        }
        try {
            countdownVoltDb.await();
        } catch (InterruptedException e) {
            System.out.println("VoltDb线程并行失败...");
            e.printStackTrace();
        }

        // System.out.println(voltDbJob.getSumLong());
        long dLong = System.currentTimeMillis() - dStart;
        System.out.println("voltDb访问线程数【" + threadCountVoltDb + "】，总执行耗时: "
                + dLong + " 毫秒 ");
        System.out.println("voltDb访问线程数【" + threadCountVoltDb + "】，平均执行耗时: "
                + voltDbJob.getSumLong() / threadCountVoltDb + " 毫秒 ");

    }

    private static void runVoltDb() {
        long dStart = System.currentTimeMillis();
        callCountStoreProduce();
        // callJoinStoreProduce();
        long dLong = System.currentTimeMillis() - dStart;
        System.out.println("voltDb执行耗时 : " + dLong + " 毫秒 ");
    }

    private static void runVoltDb(String produceName) {
        long dStart = System.currentTimeMillis();
        callStoreProduce(produceName);
        // callJoinStoreProduce();
        long dLong = System.currentTimeMillis() - dStart;
        System.out.println("voltDb执行耗时 : " + dLong + " 毫秒 ");
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

        System.out.printf("Client Single ended ---" + produceName);
    }

    private static void callCountStoreProduce() {
        System.out.println("Client started");

        ClientConfig clientConfig = new ClientConfig("program", "none");
        org.voltdb.client.Client client = org.voltdb.client.ClientFactory
                .createClient(clientConfig);

        // Client instance connected to the database running on
        // the specified IP address, in this case 127.0.0.1. The
        // database always runs on TCP/IP port 21212.
        try {
            client.createConnection("192.168.197.172");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        ClientResponse response = null;
        try {
            response = client.callProcedure(queryProceducevoltDb);
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

    private static void callJoinStoreProduce() {
        System.out.println("Client started");

        ClientConfig clientConfig = new ClientConfig("program", "none");
        org.voltdb.client.Client client = org.voltdb.client.ClientFactory
                .createClient(clientConfig);

        // Client instance connected to the database running on
        // the specified IP address, in this case 127.0.0.1. The
        // database always runs on TCP/IP port 21212.
        try {
            client.createConnection("192.168.197.172");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        ClientResponse response = null;
        try {
            response = client.callProcedure(joinProceducevoltDb);
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
}
