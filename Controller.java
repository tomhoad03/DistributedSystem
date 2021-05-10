import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;

public class Controller {
    public static int controllerPort;
    public static int replicationFactor;
    public static int timeout;
    public static int rebalancePeriod;

    public static int acks = 0;
    public static int endpoint = 0;
    public static int refreshRate = 2;
    public static boolean inRebalance = false;
    public static boolean inOperation = false;
    public static LocalDateTime lastRebalance = LocalDateTime.now();

    public static ArrayList<Datastore> datastores = new ArrayList<>();
    public static HashSet<DatastoreFile> datastoreFiles = new HashSet<>();

    public static void main(String[] args) {
        try {
            // reading arguments
            controllerPort = Integer.parseInt(args[0]);  // port to listen on
            replicationFactor = Integer.parseInt(args[1]); // replication factor
            timeout = Integer.parseInt(args[2]); // timeout wait time
            rebalancePeriod = Integer.parseInt(args[3]); // rebalance wait time

            ServerSocket controllerSocket = new ServerSocket(controllerPort);
            ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL);

            // thread for establishing new connections to clients or datastores
            Thread socketThread = new Thread(() -> {
                for (;;) {
                    try {
                        final Socket clientSocket = controllerSocket.accept();
                        new Thread(new ControllerThread(clientSocket)).start();
                        Thread.sleep(refreshRate);
                    } catch (Exception e) {
                        ControllerLogger.getInstance().log("Socket error (" + e + ")");
                    }
                }
            });
            socketThread.start();

            // thread to check if datastores need rebalancing
            Thread rebalanceThread = new Thread(() -> {
                for (;;) {
                    try {
                        if (LocalDateTime.now().isAfter(lastRebalance.plus(rebalancePeriod, ChronoUnit.MILLIS)) || inRebalance) {
                            inRebalance = true;
                            if (!inOperation) {
                                rebalanceOp();
                            }
                        }
                        Thread.sleep(refreshRate);
                    } catch (Exception e) {
                        ControllerLogger.getInstance().log("Rebalance error (" + e + ")");
                        lastRebalance = LocalDateTime.now();
                        inRebalance = false;
                    }
                }
            });
            rebalanceThread.start();
        } catch (Exception e) {
            ControllerLogger.getInstance().log("Server error (" + e + ")");
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
                    for (;;) {
                        if (line.equals("LIST")) { // list operation
                            ControllerLogger.getInstance().messageReceived(socket, line);
                            StringBuilder fileNames = new StringBuilder("LIST");
                            for (DatastoreFile datastoreFile : datastoreFiles) {
                                fileNames.append(" ").append(datastoreFile.getFileName());
                            }
                            ControllerLogger.getInstance().messageSent(socket, String.valueOf(fileNames));
                            out.println(fileNames);
                            break;
                        } else if (line.startsWith("STORE_ACK ")) { // receive store ack
                            if (!inRebalance) {
                                ControllerLogger.getInstance().messageReceived(socket, line);
                                acks++;
                            }
                            break;
                        } else if (line.startsWith("REMOVE_ACK ")) { // receive remove ack
                            if (!inRebalance) {
                                ControllerLogger.getInstance().messageReceived(socket, line);
                                acks++;
                            }
                            break;
                        }
                        ControllerLogger.getInstance().messageReceived(socket, line);
                        if (!inOperation) {
                            inOperation = true;
                            if (line.startsWith("RELOAD ")) { // reload operation
                                loadOp(line.split(" ")[1]);
                            }
                            endpoint = 0;
                            acks = 0;

                            if (line.startsWith("JOIN")) { // join operation
                                try {
                                    int port = Integer.parseInt(line.split(" ")[1]);
                                    ControllerLogger.getInstance().dstoreJoined(socket, port);
                                    datastores.add(new Datastore(port, true, socket, new HashSet<>()));
                                    inRebalance = true;
                                } catch (Exception e) {
                                    ControllerLogger.getInstance().log("Malformed join message from datastore (" + line + ")");
                                }
                            } else if (datastores.size() < replicationFactor || datastores.size() == 0) { // replication check
                                ControllerLogger.getInstance().messageSent(socket, "ERROR_NOT_ENOUGH_DSTORES");
                                out.println("ERROR_NOT_ENOUGH_DSTORES");
                            } else if (line.startsWith("STORE ")) { // store operation
                                try {
                                    storeOp(line.split(" ")[1], line.split(" ")[2]);
                                } catch (Exception e) {
                                    ControllerLogger.getInstance().log("Malformed store message from datastore (" + line + ")");
                                }
                            } else if (line.startsWith("LOAD ")) { // load operation
                                try {
                                    loadOp(line.split(" ")[1]);
                                } catch (Exception e) {
                                    ControllerLogger.getInstance().log("Malformed load message from datastore (" + line + ")");
                                }
                            } else if (line.startsWith("REMOVE")) { // remove operation
                                try {
                                    removeOp(line.split(" ")[1]);
                                } catch (Exception e) {
                                    ControllerLogger.getInstance().log("Malformed remove message from datastore (" + line + ")");
                                }
                            } else {
                                ControllerLogger.getInstance().log("Malformed and unknown message from datastore (" + line + ")");
                            }
                            break;
                        }
                        Thread.sleep(refreshRate);
                    }
                    inOperation = false;
                }
            } catch (Exception e) {
                try {
                    // dealing with disconnections
                    ControllerLogger.getInstance().log("Operation error (" + e + ")");
                    inRebalance = true;
                    inOperation = false;
                    socket.close();
                } catch (Exception ignored) { }
            }
        }

        // store operation
        public void storeOp(String fileName, String fileSize) {
            // checks if the file is already stored
            for (DatastoreFile datastoreFile : datastoreFiles) {
                if (datastoreFile.getFileName().equals(fileName)) {
                    ControllerLogger.getInstance().messageSent(socket, "ERROR_FILE_ALREADY_EXISTS");
                    out.println("ERROR_FILE_ALREADY_EXISTS");
                    return;
                }
            }

            // gets the datastore ports
            StringBuilder ports = new StringBuilder("STORE_TO");
            Comparator<Object> byNumFiles = Comparator.comparingInt(datastore -> ((Datastore) datastore).getFileNames().size());
            datastores.sort(byNumFiles);

            for (int i = 0; i < replicationFactor; i++) {
                Datastore datastore = datastores.get(i);

                if (datastore.getIndex()) {
                    ports.append(" ").append(datastore.getPort());
                    datastore.setIndex(false);
                    datastore.addFileName(fileName);
                }
            }
            ControllerLogger.getInstance().messageSent(socket, String.valueOf(ports));
            out.println(ports);

            // waiting for store acks
            LocalDateTime timeoutEnd = LocalDateTime.now().plus(timeout, ChronoUnit.MILLIS);
            for (;;) {
                LocalDateTime now = LocalDateTime.now();
                if (!now.isAfter(timeoutEnd)) {
                    if (acks == replicationFactor) {
                        ControllerLogger.getInstance().messageSent(socket, "STORE_COMPLETE");
                        out.println("STORE_COMPLETE");
                        break;
                    }
                } else {
                    ControllerLogger.getInstance().log("Not all store acks received");
                    break;
                }
            }

            // update the index for each datastore
            for (int i = 0; i < replicationFactor; i++) {
                Datastore dstore = datastores.get(i);
                dstore.setIndex(true);
            }
            datastoreFiles.add(new DatastoreFile(fileName, Integer.parseInt(fileSize)));
        }

        // load operation
        public void loadOp(String fileName) {
            // checks if the file is already stored
            boolean found = false;
            for (DatastoreFile datastoreFile : datastoreFiles) {
                if (datastoreFile.getFileName().equals(fileName)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                ControllerLogger.getInstance().messageSent(socket, "ERROR_FILE_DOES_NOT_EXIST");
                out.println("ERROR_FILE_DOES_NOT_EXIST");
                return;
            }
            int currentEndpoint = 1;

            // gets a datastore that contains the file
            for (Datastore dstore : datastores) {
                if (dstore.getFileNames().contains(fileName) && (currentEndpoint > endpoint)) {
                    for (DatastoreFile datastoreFile : datastoreFiles) {
                        if (datastoreFile.getFileName().equals(fileName)) {
                            endpoint = currentEndpoint;
                            ControllerLogger.getInstance().messageSent(socket, "LOAD_FROM " + dstore.getPort() + " " + datastoreFile.getFileSize());
                            out.println("LOAD_FROM " + dstore.getPort() + " " + datastoreFile.getFileSize());
                            return;
                        }
                    }
                } else if (currentEndpoint < datastores.size()) {
                    currentEndpoint++;
                } else {
                    ControllerLogger.getInstance().messageSent(socket, "ERROR_LOAD");
                    out.println("ERROR_LOAD");
                }
            }
        }

        // remove operation
        public void removeOp(String fileName) throws Exception {
            // checks if the file is already stored
            boolean found = false;
            for (DatastoreFile datastoreFile : datastoreFiles) {
                if (datastoreFile.getFileName().equals(fileName)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                ControllerLogger.getInstance().messageSent(socket, "ERROR_FILE_DOES_NOT_EXIST");
                out.println("ERROR_FILE_DOES_NOT_EXIST");
                return;
            }

            // removing the filename from every datastore
            for (Datastore datastore : datastores) {
                Socket dstoreSocket = new Socket(InetAddress.getLocalHost(), datastore.getPort());
                PrintWriter dstoreOut = new PrintWriter(dstoreSocket.getOutputStream(), true);

                if (datastore.getFileNames().contains(fileName)) {
                    datastore.setIndex(false);
                    ControllerLogger.getInstance().messageSent(socket, "REMOVE " + fileName);
                    dstoreOut.println("REMOVE " + fileName);
                }
            }

            // waiting for remove acks
            LocalDateTime timeoutEnd = LocalDateTime.now().plus(timeout, ChronoUnit.MILLIS);
            for (;;) {
                LocalDateTime now = LocalDateTime.now();
                if (!now.isAfter(timeoutEnd)) {
                    if (acks == replicationFactor) {
                        for (Datastore datastore : datastores) {
                            if (datastore.getFileNames().contains(fileName)) {
                                datastore.removeFileName(fileName);
                                datastore.setIndex(true);
                            }
                        }
                        ControllerLogger.getInstance().messageSent(socket, "REMOVE_COMPLETE");
                        out.println("REMOVE_COMPLETE");
                        break;
                    }
                } else {
                    ControllerLogger.getInstance().log("Not all remove acks received");
                    break;
                }
            }
            datastoreFiles.removeIf(datastoreFile -> datastoreFile.getFileName().equals(fileName));
        }
    }

    public static void rebalanceOp() throws Exception {
        inRebalance = true;

        // removes any datastores that may have disconnected
        ArrayList<Datastore> toRemove1 = new ArrayList<>();
        for (Datastore datastore : datastores) {
            if (datastore.getSocket().isClosed()) {
                toRemove1.add(datastore);
            }
        }
        for (Datastore datastore : toRemove1) {
            datastores.remove(datastore);
        }

        // gets updated list of files from the datastores
        for (Datastore datastore : datastores) {
            Socket rebalanceSocket = new Socket(InetAddress.getLocalHost(), datastore.getPort());
            BufferedReader datastoreIn = new BufferedReader(new InputStreamReader(rebalanceSocket.getInputStream()));
            PrintWriter datastoreOut = new PrintWriter(rebalanceSocket.getOutputStream(), true);

            ControllerLogger.getInstance().messageSent(datastore.getSocket(), "LIST");
            datastoreOut.println("LIST");

            String fileList = datastoreIn.readLine();
            ControllerLogger.getInstance().messageReceived(datastore.getSocket(), fileList);

            datastore.setFileNames(fileList.split(" "));
            datastore.newRebalance();

            rebalanceSocket.close();
            datastoreIn.close();
            datastoreOut.close();
        }

        // updates the list of all known files
        for (Datastore datastore : datastores) {
            for (String fileName : datastore.getFileNames()) {
                for (DatastoreFile datastoreFile : datastoreFiles) {
                    if (datastoreFile.getFileName().equals(fileName)) {
                        datastoreFile.setFound(true);
                        break;
                    }
                }
            }
        }
        ArrayList<DatastoreFile> toRemove2 = new ArrayList<>();
        for (DatastoreFile datastoreFile : datastoreFiles) {
            if (datastoreFile.isFound()) {
                datastoreFile.setFound(false);
            } else {
                toRemove2.add(datastoreFile);
            }
        }
        for (DatastoreFile datastoreFile : toRemove2) {
            datastoreFiles.remove(datastoreFile);
        }

        // check if he datastores are already balanced
        boolean balanced = true;
        int minSize = Integer.MAX_VALUE;
        int maxSize = 0;

        for (Datastore datastore : datastores) {
            if (datastore.getFileNames().size() > maxSize) {
                maxSize = datastore.getFileNames().size();
            }
            if (datastore.getFileNames().size() < minSize) {
                minSize = datastore.getFileNames().size();
            }
        }
        if (Math.abs(maxSize - minSize) > 1) {
            balanced = false;
        }
        for (DatastoreFile datastoreFile : datastoreFiles) {
            int replicationCount = 0;

            for (Datastore datastore : datastores) {
                if (datastore.getFileNames().contains(datastoreFile.getFileName())) {
                    replicationCount++;
                }
            }
            if (replicationCount != replicationFactor || !balanced) {
                balanced = false;
                break;
            }
        }

        // bin packing the datastores
        if (!balanced) {
            for (DatastoreFile datastoreFile : datastoreFiles) {
                String fileName = datastoreFile.getFileName();
                int sourcePort = 0;

                // finds the source datastores for each file
                for (Datastore datastore : datastores) {
                    if (datastore.getFileNames().contains(fileName)) {
                        datastore.newSend(fileName);
                        sourcePort = datastore.getPort();
                        break;
                    }
                }
                Comparator<Object> byRebalanceCount = Comparator.comparingInt(datastore -> ((Datastore) datastore).getRebalanceCount());
                datastores.sort(byRebalanceCount);

                // determines the destinations
                try {
                    for (int i = 0; i < (replicationFactor - 1) && i < datastores.size(); i++) {
                        Datastore datastore = datastores.get(i);

                        if (datastore.containsSendFile(fileName)) {
                            i++;
                        } else if (datastore.getFileNames().contains(fileName)) {
                            datastore.newKeep(fileName);
                            i++;
                        } else {
                            datastore.newReceive();

                            for (Datastore source : datastores) {
                                if (source.getPort() == sourcePort) {
                                    for (RebalanceFile rebalanceFile : source.getSendFiles()) {
                                        if (rebalanceFile.getFileName().equals(fileName)) {
                                            rebalanceFile.addDestinationPort(datastore.getPort());
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    ControllerLogger.getInstance().log("Error: " + e);
                }
            }
        }
        boolean redo = false;

        // send the rebalance messages to each datastore
        for (Datastore datastore : datastores) {
            Socket rebalanceSocket = new Socket(InetAddress.getLocalHost(), datastore.getPort());
            BufferedReader datastoreIn = new BufferedReader(new InputStreamReader(rebalanceSocket.getInputStream()));
            PrintWriter datastoreOut = new PrintWriter(rebalanceSocket.getOutputStream(), true);

            if (!balanced) {
                String rebalanced = datastore.finishRebalance();
                ControllerLogger.getInstance().messageSent(datastore.getSocket(), rebalanced);
                datastoreOut.println(rebalanced);
            } else {
                ControllerLogger.getInstance().messageSent(datastore.getSocket(), "REBALANCE 0 0");
                datastoreOut.println("REBALANCE 0 0");
            }

            rebalanceSocket.setSoTimeout(timeout);

            try {
                String datastoreLine = datastoreIn.readLine();
                ControllerLogger.getInstance().messageReceived(rebalanceSocket, datastoreLine);
                if (!datastoreLine.equals("REBALANCE COMPLETE")) {
                    throw new SocketTimeoutException();
                }
            } catch (SocketTimeoutException e) {
                redo = true;
                break;
            }
        }
        inRebalance = redo;
        lastRebalance = LocalDateTime.now();
    }
}

/*
1. javac Controller.java
2. java Controller 6000 1 10000 10000

3. javac Dstore.java
4a. java Dstore 6100 6000 10000 Files1
4b. run as many additional dstores as needed

5. javac Datastore.java
6. javac DatastoreFile.java
7. javac RebalanceFile.java
8. javac ClientTest.java

10. javac -cp client-1.0.2.jar ClientMain.java
11. java -cp client-1.0.2.jar;. ClientMain 6000 1000
 */
