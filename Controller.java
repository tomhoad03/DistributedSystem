import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

@SuppressWarnings({"InfiniteLoopStatement", "BusyWait"})
public class Controller {
    public static int controllerPort;
    public static int replicationFactor;
    public static int timeout;
    public static int rebalancePeriod;

    public static ArrayList<Datastore> datastores = new ArrayList<>(); // list of all datastores
    public static HashSet<String> datastoreFileNames = new HashSet<>(); // list of all the unique files stored
    public static ArrayList<String> acks = new ArrayList<>(); // list of all the acks received in the current operation

    public static String fileName;
    public static int endpointIndex;

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
                        System.out.println("join");
                        datastores.add(new Datastore(line.split(" ")[1], "available"));
                        rebalanceOp();

                    } else if (datastores.size() < replicationFactor) { // disallow client connections
                        throw new Exception("Not enough datastores connected!");

                    } else if (line.startsWith("STORE ")) { // store operation
                        System.out.println("store");
                        fileName = line.split(" ")[1];
                        storeOp();

                    } else if (line.startsWith("STORE_ACK ")) { // receive store ack from dstore operation
                        System.out.println("store ack");
                        String ackName = line.split(" ")[1];
                        if (ackName.equals(fileName)) {
                            acks.add("ACK");
                        }

                    } else if (line.startsWith("LOAD ")) { // load operation
                        System.out.println("load");
                        fileName = line.split(" ")[1];
                        endpointIndex = 0;
                        loadOp();

                    } else if (line.startsWith("RELOAD ")) {
                        System.out.println("reload");
                        fileName = line.split(" ")[1];
                        loadOp();

                    } else if (line.startsWith("REMOVE ")) { // remove operation
                        System.out.println("remove");
                        fileName = line.split(" ")[1];
                        removeOp();

                    } else if (line.startsWith("REMOVE_ACK ")) { // receive remove ack from dstore operation
                        System.out.println("remove ack");
                        String ackName = line.split(" ")[1];
                        if (ackName.equals(fileName)) {
                            acks.add("ACK");
                        } else if (ackName.equals("DOES_NOT_EXIST")) {
                            acks.add("ACK");
                            throw new Exception("File does not exist in datastore!");
                        }

                    } else if (line.equals("LIST")) { // list operation
                        System.out.println("list");
                        sendMsg(listOp());
                    }
                }
                socket.close();
            } catch (Exception e) {
                System.out.println("Error: " + e);
            }
        }

        // store operation
        public void storeOp() throws Exception {
            // checks if the file is already stored
            if (datastoreFileNames.contains(fileName)) {
                sendMsg("ERROR ALREADY_EXISTS " + fileName);
                throw new Exception("File already in datastores!");
            }
            StringBuilder ports = new StringBuilder("STORE_TO");

            // gets the datastore ports
            for (int i = 0; i < replicationFactor; i++) {
                Datastore datastore = datastores.get(i);

                if (isAvailable(datastore.getIndex())) {
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
                    sendMsg("STORE_COMPLETE");
                    break;
                }
                Thread.sleep(100);
            }

            // update the index for each datastore
            for (int i = 0; i < replicationFactor; i++) {
                Datastore datastore = datastores.get(i);

                datastore.addFileName(fileName);
                datastore.setIndex("store complete");
            }

            // ends the operation
            acks.clear();
            datastoreFileNames.add(fileName);
        }

        // load operation
        public void loadOp() throws Exception {
            // checks if the file exists
            if (!datastoreFileNames.contains(fileName)) {
                sendMsg("ERROR DOES_NOT EXIST");
                throw new Exception("File does not exist!");
            }
            int currentEndpointIndex = 0;

            // gets a datastore that contains the file
            for (Datastore datastore : datastores) {
                if (datastore.getFileNames().contains(fileName) || (currentEndpointIndex > endpointIndex)) {
                    sendMsg("LOAD_FROM " + datastore.getPort() + " 6.4mb");
                    endpointIndex = currentEndpointIndex;
                    break;
                } else {
                    // keeps track of last used datastore
                    if (currentEndpointIndex != datastores.size()) {
                        currentEndpointIndex++;
                    } else {
                        sendMsg("ERROR LOAD");
                        break;
                    }
                }
            }
        }

        // remove operation
        public void removeOp() throws Exception {
            // checks if the file is in the index
            if (!datastoreFileNames.contains(fileName)) {
                sendMsg("ERROR DOES_NOT_EXIST");
                throw new Exception("File not in index!");
            }

            // removing the filename from every datastore
            for (Datastore datastore : datastores) {
                Socket dstoreSocket = new Socket(InetAddress.getLocalHost(), datastore.getPort());
                PrintWriter dstoreOut = new PrintWriter(dstoreSocket.getOutputStream(), true);

                if (datastore.getFileNames().contains(fileName)) {
                    datastore.setIndex("remove in progress");
                    dstoreOut.println("REMOVE " + fileName);
                }
            }

            // waiting for acks
            for (;;) {
                // successful store
                if (acks.size() == replicationFactor) {
                    // removing the filename reference
                    for (Datastore datastore : datastores) {
                        if (datastore.getFileNames().contains(fileName)) {
                            datastore.removeFileName(fileName);
                            datastore.setIndex("remove complete");
                        }
                    }
                    sendMsg("REMOVE_COMPLETE");
                    break;
                }
                Thread.sleep(100);
            }
        }

        // list operation
        public String listOp() {
            datastoreFileNames.clear();

            for (Datastore datastore : datastores) {
                datastoreFileNames.addAll(datastore.getFileNames());
            }
            StringBuilder files = new StringBuilder();

            // gets the list of files
            for (String datastoreFileName : datastoreFileNames) {
                if (!(files.length() == 0)) {
                    files.append(" ");
                }
                files.append(datastoreFileName);
            }
            return files.toString();
        }

        // rebalance operation
        public void rebalanceOp() throws Exception {
            // updates current filenames for each datastore
            for (Datastore datastore : datastores) {
                Socket dstoreSocket = new Socket(InetAddress.getLocalHost(), datastore.getPort());
                BufferedReader dstoreIn = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));
                PrintWriter dstoreOut = new PrintWriter(dstoreSocket.getOutputStream(), true);

                dstoreOut.println("LIST");
                ArrayList<String> fileNames = new ArrayList<>(Arrays.asList(dstoreIn.readLine().split(" ")));

                if (!fileNames.contains("")) {
                    datastore.setFileNames(fileNames);
                } else {
                    datastore.setFileNames(new ArrayList<>());
                }
            }
            listOp();

            for (String datastoreFile : datastoreFileNames) {
                int count = 0;
                ArrayList<Integer> foundLocations = new ArrayList<>();
                ArrayList<Integer> notFoundLocations = new ArrayList<>();

                // counts how many datastore the file is stored in
                for (Datastore datastore : datastores) {
                   if (datastore.getFileNames().contains(datastoreFile)) {
                       foundLocations.add(count);
                       count++;
                   } else {
                       notFoundLocations.add(count);
                   }
                }

                // determines files to remove from the datastores
                while (count > replicationFactor) {
                    int locationOfMax = 0;
                    int numOfMax = 0;

                    // removes from the datastore with the most files
                    for (int location : foundLocations) {
                        Datastore datastore = datastores.get(location);
                        if (datastore.getNumFiles() > numOfMax) {
                            locationOfMax = location;
                            numOfMax = datastore.getNumFiles();
                        }
                    }
                    datastores.get(locationOfMax).addToRemove(datastoreFile);
                    foundLocations.remove(locationOfMax);
                    count--;
                }

                //  determines files to add to the datastores
                while (count < replicationFactor) {
                    int locationOfMin = 0;
                    int numOfMin = 0;

                    // adds to the datastore with the least files
                    for (int location : notFoundLocations) {
                        Datastore datastore = datastores.get(location);
                        if (datastore.getNumFiles() < numOfMin || numOfMin == 0) {
                            locationOfMin = location;
                            numOfMin = datastore.getNumFiles();
                        }
                    }
                    datastores.get(foundLocations.get(0)).addToSend(datastoreFile, String.valueOf(datastores.get(locationOfMin).getPort()));
                    foundLocations.remove(locationOfMin);
                    count--;
                }
            }

            // sends rebalance message to datastores
            for (Datastore datastore : datastores) {
                Socket dstoreSocket = new Socket(InetAddress.getLocalHost(), datastore.getPort());
                BufferedReader dstoreIn = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));
                PrintWriter dstoreOut = new PrintWriter(dstoreSocket.getOutputStream(), true);

                StringBuilder rebalanceMsg = new StringBuilder("REBALANCE ");
                StringBuilder filesToSend = new StringBuilder();
                StringBuilder filesToRemove = new StringBuilder();

                // files to send
                try {
                    filesToSend.append(datastore.getToSend().size());

                    for (Map.Entry<String, ArrayList<String>> entry : datastore.getToSend().entrySet()) {
                        filesToSend.append(" ").append(entry.getValue());

                        for (String toSend : entry.getValue()) {
                            filesToSend.append(" ").append(toSend);
                        }
                    }
                } catch (Exception ignored) {
                    filesToSend.append("0");
                }

                // files to remove
                try {
                    filesToSend.append(datastore.getToRemove().size());

                    for (String toRemove : datastore.getToRemove()) {
                        if (filesToRemove.length() != 0) {
                            filesToRemove.append(datastore.getToRemove().size());
                        }
                        filesToRemove.append(" ").append(toRemove);
                    }
                } catch (Exception ignored) {
                    filesToSend.append(" 0");
                }

                // send the rebalance message
                String msg = String.valueOf(rebalanceMsg.append(filesToSend).append(filesToRemove));
                dstoreOut.println(msg);
                String response = dstoreIn.readLine();

                if (response.equals("REBALANCE COMPLETE")) {
                    System.out.println("rebalance complete");
                }
            }
        }

        // checks if a file is available
        public boolean isAvailable(String index) {
            return index.equals("available") || index.equals("store complete") || index.equals("remove complete");
        }

        // sends a message to the client
        public void sendMsg(String msg) {
            out.println(msg);
        }

        // sends a message to the client, expects a response
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
 9. java Controller 6000 1 1 1
 10a. java Dstore 6100 6000 1 files1
 10b. java Dstore 6200 6000 1 files2
 11. java Client (for testing only)
 */