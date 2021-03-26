public class Dstore {
    public static void main(String[] args) {
        try {
            int dPort = Integer.parseInt(args[0]); // port to listen on
            int cPort = Integer.parseInt(args[1]); // controller port
            int timeout = Integer.parseInt(args[2]); // timeout wait time
            int fileFolder = Integer.parseInt(args[3]); // location of data store

            System.out.println("The dstore port is: " + dPort);
            System.out.println("The controller port is: " + cPort);
            System.out.println("The timeout wait time is: " + timeout);
            System.out.println("The data store location is: " + fileFolder);

        } catch (Exception e) {
            System.out.println("Invalid Arguments!");
        }
    }
}