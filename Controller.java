import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

@SuppressWarnings("InfiniteLoopStatement")
public class Controller {
    public static int controllerPort;
    public static int replicationFactor;
    public static int timeout;
    public static int rebalancePeriod;

    public static String index; // current state

    public static ArrayList<ControllerThread> dataStores = new ArrayList<>(); // list of all datastore socket threads

    public static ArrayList<String> acks = new ArrayList<>();

    public static void main(String[] args) {
        try {
            // reading arguments
            controllerPort = Integer.parseInt(args[0]);  // port to listen on
            replicationFactor = Integer.parseInt(args[1]); // replication factor
            timeout = Integer.parseInt(args[2]); // timeout wait time
            rebalancePeriod = Integer.parseInt(args[3]); // rebalance wait time

            try {
                // establish controller listener
                ServerSocket controllerSocket = new ServerSocket(controllerPort);
                for (;;) {
                    try {
                        // establish connection to new client or datastore
                        final Socket clientSocket = controllerSocket.accept();

                        ControllerThread controllerThread = new ControllerThread(clientSocket);
                        new Thread((controllerThread)).start();
                    } catch (Exception ignored) {
                    }
                }
            } catch (Exception e) { System.out.println("Error: Invalid Socket!"); }
        } catch (Exception e) { System.out.println("Error: Invalid Arguments!"); }
    }

    static class ControllerThread implements Runnable {
        public final Socket socket;
        public final BufferedReader in;
        public final PrintWriter out;
        public String line;

        public ControllerThread(Socket socket) throws Exception {
            this.socket = socket;
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.out = new PrintWriter(socket.getOutputStream());
        }

        // controller listener
        public void run() {
            try {
                while ((line = in.readLine()) != null) {
                    if (line.startsWith("JOIN")) { // establish connection to new datastore
                        dataStores.add(this);

                        /* Storage Rebalancing Operation
                        for (ControllerThread controllerThread : dataStores) {
                            controllerThread.out.println("ACK");
                            controllerThread.out.flush();
                        } */
                    } else if (dataStores.size() < replicationFactor) { // disallow client connections
                        break;
                    } else if (line.startsWith("STORE")) { // store operation
                        String fileName = line.substring(6);
                        index = "store in progress";

                        // gets the datastore threads
                        ArrayList<ControllerThread> portThreads = new ArrayList<>(dataStores.subList(0, rebalancePeriod));
                        StringBuilder ports = new StringBuilder("STORE_TO");

                        // gets the port of the sockets
                        for (ControllerThread portThread : portThreads) {
                            ports.append(" ").append(portThread.socket.getPort());
                        }

                        // return message to client
                        out.println(ports);
                        out.flush();
                        System.out.println(ports);

                        // waiting for acks
                        for (;;) {
                            try {
                                // successful store
                                if (acks.size() == portThreads.size()) {
                                    index = "store complete";
                                    acks.clear();

                                    out.println("STORE_COMPLETE");
                                    out.flush();
                                }
                            } catch (Exception e) {
                                System.out.println("Error: Invalid ACKS!");
                            }
                        }
                    } else if (line.startsWith("LOAD")) { // load operation
                        System.out.println("Load");
                    } else if (line.startsWith("REMOVE")) { // remove operation
                        System.out.println("Remove");
                    } else if (line.startsWith("LIST")) { // list operation
                        System.out.println("List");
                    }
                }
                socket.close();
            } catch (Exception e) {
                System.out.println("Error: Invalid Thread!");
            }
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

 7. Each process gets logged (more info later)

 8. Launch in terminal, Ctrl-C to close running program, remove .java with class files
 9. java Controller.java 6400 1 0 0
 10. java Dstore.java 6401 6400 0 0
 */