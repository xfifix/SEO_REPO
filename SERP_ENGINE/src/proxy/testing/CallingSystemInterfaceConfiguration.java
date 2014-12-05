package proxy.testing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class CallingSystemInterfaceConfiguration {
	// command to see the different addresses 
	// 
	private static String show_command = "sudo ip addr show";
	private static String show_command2 = "ifconfig -a";


	// command to assign an IP to a ethernet interface
	// sudo ip addr add 192.168.50.5 dev eth1
	public static void main(String[] args){	
		try {
			Process p = Runtime.getRuntime().exec(show_command);
			p.waitFor();
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line=reader.readLine();
			while (line != null) {    
				System.out.println(line);
				line = reader.readLine();
			}
		}
		catch(IOException e1) {e1.printStackTrace();}
		catch(InterruptedException e2) {e2.printStackTrace();}
		try {
			Process p = Runtime.getRuntime().exec(show_command2);
			p.waitFor();
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line=reader.readLine();

			while (line != null) {    
				System.out.println(line);
				line = reader.readLine();
			}

		}
		catch(IOException e1) {e1.printStackTrace();}
		catch(InterruptedException e2) {e2.printStackTrace();}

		System.out.println("finished.");
	}
}
