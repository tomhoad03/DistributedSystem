import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

@SuppressWarnings({"BusyWait"})
public class Controller {
    public static int controllerPort;
    public static int replicationFactor;
    public static int timeout;
    public static int rebalancePeriod;

    public static ArrayList<Datastore> datastores = new ArrayList<>(); // list of all datastores
    public static HashSet<String> datastoreFileNames = new HashSet<>(); // list of all the unique files stored
    public static ArrayList<String> acks = new ArrayList<>(); // list of all the acks received in the current operation

    public static int endpoint;
    public static int refreshRate = 100;
    public static boolean rebalanceIndex = false; // false when not rebalancing
    public static boolean controllerIndex = true; // true when available
    public static LocalDateTime lastRebalance = LocalDateTime.now();

    public static void main(String[] args) {
        try {
            // reading arguments
            controllerPort = Integer.parseInt(args[0]);  // port to listen on
            replicationFactor = Integer.parseInt(args[1]); // replication factor
            timeout = Integer.parseInt(args[2]); // timeout wait time
            rebalancePeriod = Integer.parseInt(args[3]); // rebalance wait time

            // establish controller listener
            ServerSocket controllerSocket = new ServerSocket(controllerPort);

            // thread to check if needs rebalancing
            Thread rebalanceThread = new Thread(() -> {
                for (;;) {
                    try {
                        LocalDateTime now = LocalDateTime.now();
                        LocalDateTime nextRebalance = lastRebalance.plus(rebalancePeriod, ChronoUnit.MILLIS);
                        if (now.isAfter(nextRebalance)) {
                            if (controllerIndex) {
                                rebalanceOp();
                                lastRebalance = LocalDateTime.now();
                            }
                        }
                        Thread.sleep(refreshRate);
                    } catch (Exception ignored) { }
                }
            });
            rebalanceThread.start();

            // thread for receiving new clients or datastores
            Thread socketThread = new Thread(() -> {
                for (;;) {
                    try {
                        // establish connection to new client or datastore
                        final Socket clientSocket = controllerSocket.accept();

                        ControllerThread controllerThread = new ControllerThread(clientSocket);
                        new Thread((controllerThread)).start();

                        Thread.sleep(refreshRate);
                    } catch (Exception ignored) { }
                }
            });
            socketThread.start();

        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
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
                    System.out.println(line);
                    for (;;) {
                        if (controllerIndex) {
                            controllerIndex = false;
                            if (line.startsWith("JOIN ")) { // establish connection to new datastore
                                datastores.add(new Datastore(line.split(" ")[1], "available"));
                                rebalanceOp();
                                break;
                            } else if (datastores.size() < replicationFactor) { // disallow client connections
                                out.println("ERROR_NOT_ENOUGH_DSTORES");
                                break;
                            } else if (line.startsWith("STORE ")) { // store operation
                                storeOp(line.split(" ")[1]);
                                break;
                            } else if (line.startsWith("LOAD ")) { // load operation
                                endpoint = 0;
                                loadOp(line.split(" ")[1]);
                                break;
                            } else if (line.startsWith("REMOVE ")) { // remove operation
                                removeOp(line.split(" ")[1]);
                                break;
                            } else if (line.equals("LIST")) { // list operation
                                out.println(listOp());
                                break;
                            }
                        } else if (!rebalanceIndex) {
                            if (line.startsWith("STORE_ACK ")) { // receive store ack from dstore operation
                                acks.add("ACK");
                                break;
                            } else if (line.startsWith("REMOVE_ACK ")) { // receive remove ack from dstore operation
                                String ackName = line.split(" ")[1];
                                if (datastoreFileNames.contains(ackName)) {
                                    acks.add("ACK");
                                }
                                break;
                            } else if (line.startsWith("RELOAD ")) { // reload operation
                                loadOp(line.split(" ")[1]);
                                break;
                            }
                        }
                        Thread.sleep(refreshRate);
                    }
                    controllerIndex = true;
                }
                socket.close();
            } catch (Exception e) {
                System.out.println("Error: " + e);
            }
        }

        // store operation
        public void storeOp(String fileName) throws Exception {
            // checks if the file is already stored
            if (datastoreFileNames.contains(fileName)) {
                out.println("ERROR_FILE_ALREADY_EXISTS");
                return;
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
            out.println(ports);

            // waiting for acks
            for (;;) {
                // successful store
                if (acks.size() == replicationFactor) {
                    out.println("STORE_COMPLETE");
                    break;
                }
                Thread.sleep(refreshRate);
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
        public void loadOp(String fileName) throws Exception {
            // checks if the file exists
            if (!datastoreFileNames.contains(fileName)) {
                out.println("ERROR_FILE_DOES_NOT_EXIST");
                throw new Exception("File does not exist!");
            }
            int currentEndpoint = 0;

            // gets a datastore that contains the file
            for (Datastore datastore : datastores) {
                if (datastore.getFileNames().contains(fileName) || (currentEndpoint > endpoint)) {
                    out.println("LOAD_FROM " + datastore.getPort() + " 6.4mb");
                    endpoint = currentEndpoint;
                    break;
                } else {
                    // keeps track of last used datastore
                    if (currentEndpoint != datastores.size()) {
                        currentEndpoint++;
                    } else {
                        out.println("ERROR_LOAD");
                        break;
                    }
                }
            }
        }

        // remove operation
        public void removeOp(String fileName) throws Exception {
            // checks if the file is in the index
            if (!datastoreFileNames.contains(fileName)) {
                out.println("ERROR DOES_NOT_EXIST");
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
                    out.println("REMOVE_COMPLETE");
                    break;
                }
                Thread.sleep(refreshRate);
            }

            // ends the operation
            acks.clear();
            datastoreFileNames.remove(fileName);
        }

        // checks if a file is available
        public boolean isAvailable(String index) {
            return index.equals("available") || index.equals("store complete") || index.equals("remove complete");
        }
    }

    // list operation
    public static String listOp() {
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
    public static void rebalanceOp() throws Exception {
        rebalanceIndex = true;

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
            int index = 0;
            int foundCount = 0;
            ArrayList<Integer> foundLocations = new ArrayList<>();
            ArrayList<Integer> notFoundLocations = new ArrayList<>();

            // counts how many datastore the file is stored in
            for (Datastore datastore : datastores) {
                if (datastore.getFileNames().contains(datastoreFile)) {
                    foundLocations.add(index);
                    foundCount++;
                } else {
                    notFoundLocations.add(index);
                }
                index++;
            }

            // adds to every store if there are less datastores than R
            if (datastores.size() <= replicationFactor) {
                for (int location : notFoundLocations) {
                    datastores.get(foundLocations.get(0)).addToSend(datastoreFile, String.valueOf(datastores.get(location).getPort()));
                }
            } else {
                //  determines files to add to the datastores
                while (foundCount < replicationFactor) {
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
                    foundCount--;
                }

                // determines files to remove from the datastores
                while (foundCount > replicationFactor) {
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
                    foundCount--;
                }
            }
            acks.clear();
            rebalanceIndex = false;
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
                    filesToSend.append(" ").append(entry.getKey()).append(" ").append(entry.getValue().size());

                    for (String toSend : entry.getValue()) {
                        filesToSend.append(" ").append(toSend);
                    }
                }
            } catch (Exception ignored) {
                filesToSend.append("0");
            }

            // files to remove
            try {
                filesToSend.append(" ").append(datastore.getToRemove().size());

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
 9. java Controller 6000 2 10000 10000
 10a. java Dstore 6100 6000 10000 files1
 10b. java Dstore 6200 6000 10000 files2
 11. java Client (for testing only)
 */