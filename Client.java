import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;

class Client {
    public static void main(String [] args) {
        try {
            Socket socket = new Socket("Tom-Laptop",6400);
            PrintWriter out = new PrintWriter(socket.getOutputStream());
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String line;

            out.println("STORE test.txt");
            out.flush();

            while ((line = in.readLine()) != null) {
                if (line.startsWith("STORE_TO ")) {
                    ArrayList<String> ports = new ArrayList<>(Arrays.asList(line.split(" ")));
                    ports.remove(0);

                    for (String port : ports) {
                        Socket dstoreSocket = new Socket("Tom-Laptop", Integer.parseInt(port));
                        PrintWriter clientOut = new PrintWriter(dstoreSocket.getOutputStream());
                        BufferedReader clientIn = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));
                        String clientLine;

                        clientOut.println("STORE test.txt 6.4mb");
                        clientOut.flush();

                        while ((clientLine = clientIn.readLine()) != null) {
                            if (clientLine.equals("ACK")) {
                                clientOut.println("Hello world!");
                                clientOut.flush();
                                break;
                            }
                        }
                    }
                }
            }
        }
        catch (Exception e) {
            System.out.println("error" + e);
        }
    }
}