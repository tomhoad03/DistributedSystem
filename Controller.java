import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

@SuppressWarnings("InfiniteLoopStatement")
public class Controller {
    public static void main(String[] args) {
        try {
            // reading arguments
            int controllerPort = Integer.parseInt(args[0]);  // port to listen on
            int replicationFactor = Integer.parseInt(args[1]); // replication factor
            int timeout = Integer.parseInt(args[2]); // timeout wait time
            int rebalancePeriod = Integer.parseInt(args[3]); // rebalance wait time

            try {
                // establish controller listener
                ServerSocket controllerSocket = new ServerSocket(controllerPort);
                for (;;) {
                    try {
                        // establish connection to client
                        Socket clientSocket = controllerSocket.accept();
                        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                        String line;

                        // receive messages from client
                        while ((line = in.readLine()) != null) {
                            System.out.println(line + " received");
                        }
                        clientSocket.close();
                    } catch (Exception ignored) { }
                }
            } catch (Exception e) {
                System.out.println("Error: Invalid Socket!");
            }
        } catch (Exception e) {
            System.out.println("Error: Invalid Arguments!");
        }
    }
}

/*
 Notes:
 1. Client is given, create controller and dstores

 2. Store - client stores files in every datastore
 3. Load - controller gives client requested file (from one datastore)
 4. Remove - file removed from every datastore
 5. List - gets a list of all the files
 6. Storage Rebalancing - each file is stored in r dstores and files are evenly stored (when adding a new dstore and after interval)

 . Launch in terminal, Ctrl-C to close running program
 */