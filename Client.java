package com.jd.orderpat.client;

// VoltTable is VoltDB's table representation.
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientResponse;

public class Client {
    
    public static void main(String[] args) throws Exception {
        System.out.println("Client started");
        
        ClientConfig clientConfig = new ClientConfig("program", "none");
        org.voltdb.client.Client client =
            org.voltdb.client.ClientFactory.createClient(clientConfig);
        
        // Client instance connected to the database running on
        // the specified IP address, in this case 127.0.0.1. The
        // database always runs on TCP/IP port 21212.
        client.createConnection("Hadoop-sandbox-08.pekdc1.jdfin.local");
        
        // Example of executing a dynamic SQL query
        String tableName = "TOWNS";
        //VoltTable[] count = client.callProcedure("@AdHoc", "SELECT COUNT(*) FROM " + tableName).getResults();
        //System.out.printf("Found %d employees.\n", count[0].fetchRow(0).getLong(0));
        
        
        // Synchronous call to find out what department Larry Merchant works in
        ClientResponse response = client.callProcedure("QueryCount");
        if (response.getStatus() != ClientResponse.SUCCESS) {
        	System.err.println(response.getStatusString());
        	System.exit(-1);
        }
        
        final VoltTable results[] = response.getResults();
       
        VoltTable result = results[0];
        VoltTableRow row = result.fetchRow(0);
        System.out.printf("------ Total recourds is %d\n", row.getLong(0));
    }

}
