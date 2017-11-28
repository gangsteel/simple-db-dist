package networking;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Created by aditisri on 11/27/17.
 */
public class DeleteNodeRequest implements Runnable{
    private String toChildIp;
    private String deleteChildIp;
    private int toChildPort;
    private int deleteChildPort;
    public DeleteNodeRequest(String toChildIp, int toChildPort, String deleteChildIp, int deleteChildPort){
        this.toChildIp = toChildIp;
        this.toChildPort = toChildPort;
        this.deleteChildIp = deleteChildIp;
        this.deleteChildPort = deleteChildPort;
    }

    @Override
    public void run(){
        try {
            Socket s = new Socket(this.toChildIp, this.toChildPort);
            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
            PrintWriter out = new PrintWriter(s.getOutputStream(), true);
            out.println("DELETE NODE " + this.deleteChildIp + " " + this.deleteChildPort);
            for(String line=in.readLine();!line.equals("DONE"); line=in.readLine()){
                //TODO: handle input
                System.out.println(line);
            }
            //Yay! It's done!
            in.close();
            out.close();
            s.close();
        }
        catch(IOException e) {
            e.printStackTrace();
            //TODO: do error handling
        }
    }
}
