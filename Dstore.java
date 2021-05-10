import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

public class Dstore {
    public static int datastorePort;
    public static int controllerPort;
    public static int timeout;
    public static String fileFolder;

    public static int refreshRate = 2;
    public static boolean inRebalance = false;

    public static DstoreThread datastoreThread;

    public static void main(String[] args) {
        try {
            // reading arguments
            datastorePort = Integer.parseInt(args[0]); // port to listen on/
            controllerPort = Integer.parseInt(args[1]); // controller port
            timeout = Integer.parseInt(args[2]); // timeout wait time
            fileFolder = args[3]; // location of data store

            ServerSocket datastoreSocket = new ServerSocket(datastorePort);
            DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, datastorePort);

            // establish connection to controller
            datastoreThread = new DstoreThread(new Socket(InetAddress.getLocalHost(), controllerPort));
            new Thread(datastoreThread).start();
            datastoreThread.joinController();

            // thread for establishing new connections to clients
            Thread socketThread = new Thread(() -> {
                for (;;) {
                    try {
                        // establish new connection to client
                        final Socket clientSocket = datastoreSocket.accept();
                        new Thread(new DstoreThread(clientSocket)).start();

                        Thread.sleep(refreshRate);
                    } catch (Exception e) {
                        DstoreLogger.getInstance().log("Socket error (" + e + ")");
                    }
                }
            });
            socketThread.start();
        } catch (Exception e) {
            DstoreLogger.getInstance().log("Server error (" + e + ")");
        }
    }

    static class DstoreThread implements Runnable {
        private final Socket socket;
        private final BufferedReader in;
        private final PrintWriter out;

        public DstoreThread(Socket socket) throws Exception {
            this.socket = socket;
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.out = new PrintWriter(socket.getOutputStream(), true);
        }

        public void run() {
            try {
                String line;
                while ((line = in.readLine()) != null) {
                    DstoreLogger.getInstance().messageReceived(socket, line);

                    if (line.startsWith("STORE ")) { // store operation
                        try {
                            String fileName = line.split(" ")[1];
                            String fileSize = line.split(" ")[2];

                            // send ack to client and get file contents
                            DstoreLogger.getInstance().messageSent(socket, "ACK");
                            out.println("ACK");

                            byte[] fileContents = socket.getInputStream().readNBytes(Integer.parseInt(fileSize));
                            File file = new File(fileFolder + File.separator + fileName);
                            file.getParentFile().mkdirs();
                            file.createNewFile();
                            Files.write(file.toPath(), fileContents);

                            // send ack to controller
                            DstoreLogger.getInstance().log(String.valueOf(inRebalance));
                            if (socket.getPort() == datastorePort || socket.getLocalPort() == datastorePort && !inRebalance) {
                                DstoreLogger.getInstance().messageSent(datastoreThread.socket, "STORE_ACK " + fileName);
                                datastoreThread.out.println("STORE_ACK " + fileName);
                            }
                        } catch (Exception e) {
                            DstoreLogger.getInstance().log("Malformed store message from the client (" + line + ")");
                        }
                    } else if (line.startsWith("LOAD_DATA ")) { // load operation
                        boolean found = false;
                        try {
                            String fileName = line.split(" ")[1];

                            for (File file : Objects.requireNonNull(new File(fileFolder).listFiles())) {
                                if (file.getName().equals(fileName)) {
                                    DstoreLogger.getInstance().messageSent(socket, Arrays.toString(Files.readAllBytes(file.toPath())));
                                    socket.getOutputStream().write(Files.readAllBytes(file.toPath()));
                                    found = true;
                                    break;
                                }
                            }
                        } catch (Exception e) {
                            DstoreLogger.getInstance().log("Malformed load message from the client (" + line + ")");
                        }
                        if (!found) {
                            DstoreLogger.getInstance().messageSent(datastoreThread.socket, "ERROR_FILE_DOES_NOT_EXIST");
                            out.println("ERROR_FILE_DOES_NOT_EXIST");
                        }
                    } else if (line.startsWith("REMOVE ")) {
                        boolean found = false;
                        try {
                            String fileName = line.split(" ")[1];

                            for (File file : Objects.requireNonNull(new File(fileFolder).listFiles())) {
                                if (file.getName().equals(fileName)) {
                                    Files.delete(file.toPath());
                                    found = true;
                                    break;
                                }
                            }
                            if (found) {
                                DstoreLogger.getInstance().messageSent(datastoreThread.socket, "REMOVE_ACK " + fileName);
                                datastoreThread.out.println("REMOVE_ACK " + fileName);
                            }
                        } catch (Exception e) {
                            DstoreLogger.getInstance().log("Malformed remove message from the client (" + line + ")");
                        }
                    } else if (line.equals("LIST")) { // list operation
                        StringBuilder files = new StringBuilder();

                        for (File file : Objects.requireNonNull(new File(fileFolder).listFiles())) {
                            if (!(files.length() == 0)) {
                                files.append(" ");
                            }
                            files.append(file.getName());
                        }
                        DstoreLogger.getInstance().messageSent(socket, String.valueOf(files));
                        out.println(files);
                    } else if (line.startsWith("REBALANCE ")) {
                        ArrayList<String> splitLine = new ArrayList<>(Arrays.asList(line.split(" ")));
                        int numSends = Integer.parseInt(splitLine.get(1));
                        inRebalance = true;
                        int count = 2;

                        // file sending
                        for (int i = 0; i <= numSends - 1; i++) {
                            String fileName = splitLine.get(count);
                            int numPorts = Integer.parseInt(splitLine.get(count + 1));
                            ArrayList<String> ports = new ArrayList<>(splitLine.subList(count + 2, count + numPorts + 2));

                            // send file to ports
                            for (String port : ports) {
                                Socket datastoreSocket = new Socket(InetAddress.getLocalHost(), Integer.parseInt(port));
                                BufferedReader datastoreIn = new BufferedReader(new InputStreamReader(datastoreSocket.getInputStream()));
                                PrintWriter datastoreOut = new PrintWriter(datastoreSocket.getOutputStream(), true);

                                for (File file : Objects.requireNonNull(new File(fileFolder).listFiles())) {
                                    if (file.getName().equals(fileName)) {
                                        DstoreLogger.getInstance().messageSent(socket, "STORE " + fileName + " " + file.length());
                                        datastoreOut.println("STORE " + fileName + " " + file.length());

                                        String datastoreLine;
                                        while ((datastoreLine = datastoreIn.readLine()) != null) {
                                            DstoreLogger.getInstance().messageReceived(socket, datastoreLine);

                                            if (datastoreLine.equals("ACK")) {
                                                DstoreLogger.getInstance().messageSent(socket, Arrays.toString(Files.readAllBytes(file.toPath())));
                                                datastoreSocket.getOutputStream().write(Files.readAllBytes(file.toPath()));
                                                break;
                                            }
                                        }
                                        break;
                                    }
                                }
                                datastoreSocket.close();
                            }
                            count = count + numPorts + 2;
                        }

                        // file removing
                        if (Integer.parseInt(splitLine.get(count)) > 0) {
                            ArrayList<String> toRemove = new ArrayList<>(splitLine.subList(count + 1, splitLine.size()));

                            for (String fileName : toRemove) {
                                for (File file : Objects.requireNonNull(new File(fileFolder).listFiles())) {
                                    if (file.getName().equals(fileName)) {
                                        Files.delete(file.toPath());
                                        break;
                                    }
                                }
                            }
                        }
                        DstoreLogger.getInstance().messageSent(socket,"REBALANCE COMPLETE");
                        out.println("REBALANCE COMPLETE");
                        inRebalance = false;
                    }
                }
            } catch (Exception e) {
                DstoreLogger.getInstance().log("Operation error (" + e + ")");
                inRebalance = false;
            }
        }

        public void joinController() throws Exception {
            File folder = new File(fileFolder);
            if (!folder.exists()) {
                boolean mkdir = folder.mkdir();
                if (!mkdir) {
                    throw new Exception("Cannot join controller");
                }
            }
            DstoreLogger.getInstance().messageSent(socket, "JOIN " + datastorePort);
            out.println("JOIN " + datastorePort);
        }
    }
}