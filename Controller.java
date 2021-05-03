import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
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
    public static ControllerLogger controllerLogger;
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

            controllerLogger = new ControllerLogger(Logger.LoggingType.ON_TERMINAL_ONLY);

            // establish controller listener
            ServerSocket controllerSocket = new ServerSocket(controllerPort);

            // thread for receiving new clients or datastores
            Thread socketThread = new Thread(() -> {
                for (;;) {
                    try {
                        // establish connection to new client or datastore
                        final Socket clientSocket = controllerSocket.accept();
                        new Thread(new ControllerThread(clientSocket)).start();

                        Thread.sleep(refreshRate);
                    } catch (Exception e) {
                        controllerLogger.log("Socket error (" + e + ")");
                    }
                }
            });
            socketThread.start();

            // thread to check if needs rebalancing
            Thread rebalanceThread = new Thread(() -> {
                for (;;) {
                    try {
                        if (LocalDateTime.now().isAfter(lastRebalance.plus(rebalancePeriod, ChronoUnit.MILLIS)) || inRebalance) {
                            inRebalance = true;

                            if (!inOperation) {
                                System.out.println("Rebalancing");

                                lastRebalance = LocalDateTime.now();
                                inRebalance = false;
                            }
                        }
                        Thread.sleep(refreshRate);
                    } catch (Exception e) {
                        controllerLogger.log("Rebalance error (" + e + ")");
                    }
                }
            });
            rebalanceThread.start();

        } catch (Exception e) {
            controllerLogger.log("Server error (" + e + ")");
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
                    controllerLogger.messageReceived(socket, line);

                    for (;;) {
                        if (line.startsWith("STORE_ACK ")) {
                            acks++;
                            break;
                        } else if (line.startsWith("REMOVE_ACK ")) {
                            acks++;
                            break;
                        } else if (!inOperation && !inRebalance) {
                            inOperation = true;
                            if (line.startsWith("RELOAD ")) {
                                loadOp(line.split(" ")[1]);
                            }
                            endpoint = 0;
                            acks = 0;

                            if (line.startsWith("JOIN")) {
                                try {
                                    int port = Integer.parseInt(line.split(" ")[1]);
                                    controllerLogger.dstoreJoined(socket, port);
                                    datastores.add(new Datastore(port, true, new ArrayList<>()));
                                    inRebalance = true;
                                } catch (Exception e) {
                                    controllerLogger.log("Malformed join message from datastore (" + line + ")");
                                }
                            } else if (datastores.size() < replicationFactor || datastores.size() == 0) {
                                controllerLogger.messageSent(socket, "ERROR_NOT_ENOUGH_DSTORES");
                                out.println("ERROR_NOT_ENOUGH_DSTORES");
                            } else if (line.startsWith("STORE ")) {
                                try {
                                    storeOp(line.split(" ")[1], line.split(" ")[2]);
                                } catch (Exception e) {
                                    controllerLogger.log("Malformed store message from datastore (" + line + ")");
                                }
                            } else if (line.startsWith("LOAD ")) {
                                try {
                                    loadOp(line.split(" ")[1]);
                                } catch (Exception e) {
                                    controllerLogger.log("Malformed load message from datastore (" + line + ")");
                                }
                            } else if (line.startsWith("REMOVE")) {
                                try {
                                    removeOp(line.split(" ")[1]);
                                } catch (Exception e) {
                                    controllerLogger.log("Malformed remove message from datastore (" + line + ")");
                                }
                                System.out.println(line);

                            } else if (line.equals("LIST")) {
                                StringBuilder fileNames = new StringBuilder("LIST");
                                for (DatastoreFile datastoreFile : datastoreFiles) {
                                    fileNames.append(" ").append(datastoreFile.getFileName());
                                }
                                out.println(fileNames);
                            } else {
                                controllerLogger.log("Malformed and unknown message from datastore (" + line + ")");
                            }
                            break;
                        }
                        Thread.sleep(refreshRate);
                    }
                    inOperation = false;
                }
            } catch (Exception e) {
                controllerLogger.log("Operation error (" + e + ")");
                inRebalance = true;
                inOperation = false;
            }
        }

        // store operation
        public void storeOp(String fileName, String fileSize) {
            // checks if the file is already stored
            for (DatastoreFile datastoreFile : datastoreFiles) {
                if (datastoreFile.getFileName().equals(fileName)) {
                    controllerLogger.messageSent(socket, "ERROR_FILE_ALREADY_EXISTS");
                    out.println("ERROR_FILE_ALREADY_EXISTS");
                    return;
                }
            }
            StringBuilder ports = new StringBuilder("STORE_TO");

            // gets the datastore ports
            for (int i = 0; i < replicationFactor; i++) {
                Datastore datastore = datastores.get(i);

                if (datastore.getIndex()) {
                    ports.append(" ").append(datastore.getPort());
                    datastore.setIndex(false);
                    datastore.addFileName(fileName);
                }
            }
            controllerLogger.messageSent(socket, String.valueOf(ports));
            out.println(ports);

            // waiting for acks
            LocalDateTime timeoutEnd = LocalDateTime.now().plus(timeout, ChronoUnit.MILLIS);
            for (;;) {
                LocalDateTime now = LocalDateTime.now();
                if (!now.isAfter(timeoutEnd)) {
                    // successful store
                    if (acks == replicationFactor) {
                        controllerLogger.messageSent(socket, "STORE_COMPLETE");
                        out.println("STORE_COMPLETE");
                        break;
                    }
                } else {
                    controllerLogger.log("Store acks error (not all acks received)");
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
            // checks if the file exists
            boolean found = false;

            for (DatastoreFile datastoreFile : datastoreFiles) {
                if (datastoreFile.getFileName().equals(fileName)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                controllerLogger.messageSent(socket, "ERROR_FILE_DOES_NOT_EXIST");
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
                            controllerLogger.messageSent(socket, "LOAD_FROM " + dstore.getPort() + " " + datastoreFile.getFileSize());
                            out.println("LOAD_FROM " + dstore.getPort() + " " + datastoreFile.getFileSize());
                            break;
                        }
                    }
                } else if (currentEndpoint < datastores.size()) {
                    currentEndpoint++;
                } else {
                    controllerLogger.messageSent(socket, "ERROR_LOAD");
                    out.println("ERROR_LOAD");
                }
            }
        }

        // remove operation
        public void removeOp(String fileName) throws Exception {
            // checks if the file is in the index
            boolean found = false;
            for (DatastoreFile datastoreFile : datastoreFiles) {
                if (datastoreFile.getFileName().equals(fileName)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                controllerLogger.messageSent(socket, "ERROR_FILE_DOES_NOT_EXIST");
                out.println("ERROR_FILE_DOES_NOT_EXIST");
                return;
            }

            // removing the filename from every datastore
            for (Datastore datastore : datastores) {
                Socket dstoreSocket = new Socket(InetAddress.getLocalHost(), datastore.getPort());
                PrintWriter dstoreOut = new PrintWriter(dstoreSocket.getOutputStream(), true);

                if (datastore.getFileNames().contains(fileName)) {
                    datastore.setIndex(false);
                    controllerLogger.messageSent(socket, "REMOVE " + fileName);
                    dstoreOut.println("REMOVE " + fileName);
                }
            }

            // waiting for acks
            LocalDateTime timeoutEnd = LocalDateTime.now().plus(timeout, ChronoUnit.MILLIS);
            for (;;) {
                LocalDateTime now = LocalDateTime.now();
                if (!now.isAfter(timeoutEnd)) {
                    // successful store
                    if (acks == replicationFactor) {
                        // removing the filename reference
                        for (Datastore datastore : datastores) {
                            if (datastore.getFileNames().contains(fileName)) {
                                datastore.removeFileName(fileName);
                                datastore.setIndex(true);
                            }
                        }
                        controllerLogger.messageSent(socket, "REMOVE_COMPLETE");
                        out.println("REMOVE_COMPLETE");
                        break;
                    }
                } else {
                    controllerLogger.log("Remove acks error (not all acks received)");
                    break;
                }
            }
            datastoreFiles.removeIf(datastoreFile -> datastoreFile.getFileName().equals(fileName));
        }
    }
}
