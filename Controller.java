import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

@SuppressWarnings({"InfiniteLoopStatement", "BusyWait"})
public class Controller {
    public static int controllerPort;
    public static int replicationFactor;
    public static int timeout;
    public static int rebalancePeriod;

    public static ArrayList<Datastore> dataStores = new ArrayList<>(); // list of all datastores
    public static ArrayList<String> acks = new ArrayList<>(); // list of all the acks received in the current operation

    public static ArrayList<DatastoreFile> controllerFiles = new ArrayList<>(); // list of all the files stored
    public static String fileName;
    public static String fileSize;

    public static void main(String[] args) {
        try {
            // reading arguments
            controllerPort = Integer.parseInt(args[0]);  // port to listen on
            replicationFactor = Integer.parseInt(args[1]); // replication factor
            timeout = Integer.parseInt(args[2]); // timeout wait time
            rebalancePeriod = Integer.parseInt(args[3]); // rebalance wait time

            // establish controller listener
            ServerSocket controllerSocket = new ServerSocket(controllerPort);
            for (;;) {
                try {
                    // establish connection to new client or datastore
                    final Socket clientSocket = controllerSocket.accept();

                    ControllerThread controllerThread = new ControllerThread(clientSocket);
                    new Thread((controllerThread)).start();

                    Thread.sleep(100);
                } catch (Exception ignored) { }
            }
        } catch (Exception e) { System.out.println("Error: " + e); }
    }

    static class ControllerThread implements Runnable {
        private final Socket socket;
        private final BufferedReader in;
        private final PrintWriter out;

        public ControllerThread(Socket socket) throws Exception {
            this.socket = socket;
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.out = new PrintWriter(socket.getOutputStream(), true);
        }

        // controller listener
        public void run() {
            try {
                String line;
                while ((line = in.readLine()) != null) {
                    if (line.startsWith("JOIN ")) { // establish connection to new datastore
                        dataStores.add(new Datastore(line.split(" ")[1], "available"));

                    } else if (dataStores.size() < replicationFactor) { // disallow client connections
                        throw new Exception("Not enough datastores connected!");

                    } else if (line.startsWith("STORE ")) { // store operation
                        fileName = line.split(" ")[1];

                        // checks if the file is already stored
                        if (indexContains(fileName)) {
                            sendMsg("ERROR ALREADY_EXISTS " + fileName);
                            throw new Exception("File already in datastores!");
                        }
                        StringBuilder ports = new StringBuilder("STORE_TO");

                        // gets the datastore ports
                        for (int i = 0; i < replicationFactor; i++) {
                            Datastore datastore = dataStores.get(i);

                            if (datastore.getIndex().equals("available")) {
                                ports.append(" ").append(datastore.getPort());
                                datastore.setIndex("store in progress");
                            } else {
                                throw new Exception("Datastores unavailable!");
                            }
                        }
                        sendMsg(ports.toString());

                        // waiting for acks
                        for (;;) {
                            // successful store
                            if (acks.size() == replicationFactor) {
                                out.println("STORE_COMPLETE");
                                out.flush();
                                break;
                            }
                            Thread.sleep(100);
                        }

                        // update the index for each datastore
                        for (int i = 0; i < replicationFactor; i++) {
                            Datastore datastore = dataStores.get(i);

                            datastore.addFileName(fileName);
                            datastore.setIndex("available");
                        }

                        // ends the operation
                        acks.clear();
                        controllerFiles.add(new DatastoreFile(fileName, fileSize));

                    } else if (line.startsWith("STORE_ACK ")) { // receive ack from dstore operation
                        String ackName = line.split(" ")[1];
                        if (ackName.equals(fileName)) {
                            acks.add("ACK");
                        }

                    } else if (line.startsWith("LOAD" )) { // load operation
                        fileName = line.split(" ")[1];

                        // checks if the file exists
                        if (!indexContains(fileName)) {
                            sendMsg("ERROR DOES_NOT EXIST");
                            throw new Exception("File does not exist!");
                        }

                        // gets a datastore that contains the file
                        for (Datastore datastore : dataStores) {
                            if (datastore.getFileNames().contains(fileName)) {
                                sendMsg("LOAD_FROM " + datastore.getPort() + " 6.4mb");
                            }
                        }

                    } else if (line.startsWith("REMOVE ")) { // remove operation
                        System.out.println("Remove");

                    } else if (line.startsWith("LIST ")) { // list operation
                        System.out.println("List");
                    }
                }
                socket.close();
            } catch (Exception e) {
                System.out.println("Error: " + e);
            }
        }

        // checks if a file is in the index
        public boolean indexContains(String fileName) {
            for (DatastoreFile datastoreFile : controllerFiles) {
                if (datastoreFile.getFileName().equals(fileName)) {
                    return true;
                }
            }
            return false;
        }

        public void sendMsg(String msg) {
            out.println(msg);
        }

        public String sendMsgReceiveMsg(String msg) throws Exception {
            out.println(msg);
            return in.readLine();
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
 11. java Client.java (for testing only)
 */