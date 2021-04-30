import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;

public class Controller {
    public static int controllerPort;
    public static int replicationFactor;
    public static int timeout;
    public static int rebalancePeriod;

    public static int refreshRate = 2;
    public static boolean inRebalance = false;
    public static boolean inOperation = false;
    public static LocalDateTime lastRebalance = LocalDateTime.now();

    public static ArrayList<DstoreClass> dstores = new ArrayList<>();

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

                            if (line.startsWith("JOIN")) {
                                try {
                                    String port = line.split(" ")[1];
                                    dstores.add(new DstoreClass(Integer.parseInt(port), true, new ArrayList<>()));
                                    System.out.println(port);
                                    out.println(line);

                                } catch (Exception e) {
                                    System.out.println("Malformed joined message");
                                }

                                inRebalance = true;
                            } else if (dstores.size() < replicationFactor) {
                                out.println("ERROR_NOT_ENOUGH_DSTORES");

                            } else if (line.startsWith("STORE ")) {
                                System.out.println(line);
                                out.println(line);

                            } else if (line.startsWith("LOAD ")) {
                                System.out.println(line);
                                out.println(line);

                            } else if (line.startsWith("REMOVE")) {
                                System.out.println(line);
                                out.println(line);

                            } else if (line.startsWith("LIST")) {
                                System.out.println(line);
                                out.println(line);

                            }
                            break;
                        }
                        Thread.sleep(refreshRate);
                    }

                    System.out.println("Finished: " + line);
                    inOperation = false;
                }
            } catch (Exception e) { System.out.println("Operation Error: " + e); }
        }
    }
}
