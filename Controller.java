import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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
    public static int refreshRate = 2;
    public static boolean inRebalance = false;
    public static boolean inOperation = false;
    public static LocalDateTime lastRebalance = LocalDateTime.now();

    public static ArrayList<Datastore> dstores = new ArrayList<>();
    public static HashSet<String> dstoreFileNames = new HashSet<>();

    public static void main(String[] args) {
        try {
            // reading arguments
            controllerPort = Integer.parseInt(args[0]);  // port to listen on
            replicationFactor = Integer.parseInt(args[1]); // replication factor
            timeout = Integer.parseInt(args[2]); // timeout wait time
            rebalancePeriod = Integer.parseInt(args[3]); // rebalance wait time

            // establish controller listener
            ServerSocket controllerSocket = new ServerSocket(controllerPort);

            // thread for receiving new clients or datastores
            Thread socketThread = new Thread(() -> {
                for (;;) {
                    try {
                        // establish connection to new client or datastore
                        final Socket clientSocket = controllerSocket.accept();
                        new Thread((new ControllerThread(clientSocket))).start();

                        Thread.sleep(refreshRate);
                    } catch (Exception e) { System.out.println("Socket Error: " + e);}
                }
            });
            socketThread.start();

            // thread to check if needs rebalancing
            Thread rebalanceThread = new Thread(() -> {
                for (;;) {
                    try {
                        LocalDateTime now = LocalDateTime.now();
                        LocalDateTime nextRebalance = lastRebalance.plus(rebalancePeriod, ChronoUnit.MILLIS);
                        if (now.isAfter(nextRebalance) || inRebalance) {
                            inRebalance = true;

                            if (!inOperation) {
                                System.out.println("Rebalancing");

                                lastRebalance = LocalDateTime.now();
                                inRebalance = false;
                            }
                        }
                        Thread.sleep(refreshRate);
                    } catch (Exception ignored) { }
                }
            });
            rebalanceThread.start();

        } catch (Exception e) { System.out.println("Server Error: " + e); }
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
                    System.out.println("Received: " + line);

                    for (;;) {
                        if (!inOperation && !inRebalance) {
                            inOperation = true;
                            acks = 0;

                            if (line.startsWith("JOIN")) {
                                try {
                                    dstores.add(new Datastore(Integer.parseInt(line.split(" ")[1]), true, new ArrayList<>()));
                                    inRebalance = true;

                                } catch (Exception e) {
                                    System.out.println("Log: Malformed joined message");
                                }

                            } else if (dstores.size() < replicationFactor) {
                                out.println("ERROR_NOT_ENOUGH_DSTORES");

                            } else if (line.startsWith("STORE ")) {
                                storeOp(line.split(" ")[1]);
                                System.out.println(line);

                            } else if (line.startsWith("LOAD ")) {
                                System.out.println(line);

                            } else if (line.startsWith("REMOVE")) {
                                System.out.println(line);

                            } else if (line.startsWith("LIST")) {
                                System.out.println(line);

                            }
                            break;
                        }
                        if (line.startsWith("STORE_ACK ")) { // receive store ack from dstore operation
                            acks++;
                            break;
                        }
                        Thread.sleep(refreshRate);
                    }

                    System.out.println("Finished: " + line);
                    inOperation = false;
                }
            } catch (Exception e) { System.out.println("Operation Error: " + e); }
        }

        // store operation
        public void storeOp(String fileName) {
            // checks if the file is already stored
            if (dstoreFileNames.contains(fileName)) {
                out.println("ERROR_FILE_ALREADY_EXISTS");
                return;
            }
            StringBuilder ports = new StringBuilder("STORE_TO");

            // gets the datastore ports
            for (int i = 0; i < replicationFactor; i++) {
                Datastore dstore = dstores.get(i);

                if (dstore.getIndex()) {
                    ports.append(" ").append(dstore.getPort());
                    dstore.setIndex(false);
                } else {
                    System.out.println("Datastore " + dstore.getPort() + " unavailable");
                }
            }
            out.println(ports);

            // waiting for acks
            LocalDateTime timeoutEnd = LocalDateTime.now().plus(timeout, ChronoUnit.MILLIS);
            for (;;) {
                LocalDateTime now = LocalDateTime.now();
                if (!now.isAfter(timeoutEnd)) {
                    // successful store
                    if (acks == replicationFactor) {
                        out.println("STORE_COMPLETE");
                        break;
                    }
                } else {
                    System.out.println("Log: not all store acks received");
                    break;
                }
            }

            // update the index for each datastore
            for (int i = 0; i < replicationFactor; i++) {
                Datastore dstore = dstores.get(i);
                dstore.setIndex(true);
            }
            dstoreFileNames.add(fileName);
        }
    }
}
