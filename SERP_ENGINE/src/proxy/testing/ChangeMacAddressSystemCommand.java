package proxy.testing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Random;

public class ChangeMacAddressSystemCommand {
	//sudo ifconfig <interface> down
	//Replace the <interface> field with the appropriate name of the interface, for example eth0 or wlan0.
	//Step 2: Change the MAC address of the interface with the following command:
	private static String putting_down = "sudo ifconfig eth0 down";

	//sudo ifconfig <interface> hw addr <new_MAC_address>   
	//Just make sure that the new MAC address you will be entering is 12 digits in length with the format aa:bb:cc:dd:ee:ff.
	//Step 3: Enable the interface using following command:
	// we have to randomly generate the new MAC address
	private static String assigning_new_adress = "sudo ifconfig eht0 hw addr ";//+ " <new_MAC_address>";

	//sudo ifconfig <interface> up	
	private static String putting_up = "sudo ifconfig eth0 up";

	// we also can change the mac using the macchanger software
	//sudo apt-get install macchanger macchanger-gtk
	// macchanger -A eth1
	// macchanger -r eth1
	// macchanger --mac=01:23:45:67:89:AB eth1
	// ./macchanger --list=Cray
	private static String mac_changing = "macchanger eth0";
	private static String mac_changing_just_the_end = "macchanger --endding eth0";
	private static String mac_changing_all = "macchanger --another eth0";
		
	public static void main(String[] args){
		// putting down
		try {
			Process p = Runtime.getRuntime().exec(putting_down);
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

		// changing
		try {
			Process p = Runtime.getRuntime().exec(assigning_new_adress+generatePartialRandomMACAddress(getOrganisationalStub()));
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


		// putting up
		try {
			Process p = Runtime.getRuntime().exec(putting_up);
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

	}

	private static String getMacAddress(){
		String stub = "";
		InetAddress ip;
		try {
			ip = InetAddress.getLocalHost();
			System.out.println("Current IP address : " + ip.getHostAddress());
			NetworkInterface network = NetworkInterface.getByInetAddress(ip);
			byte[] mac = network.getHardwareAddress();
			System.out.print("Current MAC address : ");
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < mac.length; i++) {
				sb.append(String.format("%02x%s", mac[i], (i < mac.length - 1) ? ":" : ""));		
			}
			System.out.println(sb.toString());
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (SocketException e){
			e.printStackTrace();
		}
		return stub;
	}

	private static String getOrganisationalStub(){
		String stub = "";
		InetAddress ip;
		int stub_length = 3;
		try {
			ip = InetAddress.getLocalHost();
			NetworkInterface network = NetworkInterface.getByInetAddress(ip);
			byte[] mac = network.getHardwareAddress();
			StringBuilder sb = new StringBuilder();
			// we here just keep the first three bytes (6 hexadecimals numbers)
			for (int i = 0; i < stub_length; i++) {
				sb.append(String.format("%02x%s", mac[i], (i < stub_length - 1) ? ":" : ""));		
			}
			stub=sb.toString();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (SocketException e){
			e.printStackTrace();
		}
		return stub;
	}

	private static String generateFullRandomMACAddress(){
		//Most of us are aware of the fact that, 
		//every Network Interface Card (NIC) in the world comes with a unique identity,
		//given by its manufacturer, which is popularly known as "MAC Address" or "Ethernet Hardware address" or "Physical Address". 
		//These are 48-bits (12 hexadecimal digits) in length and stored in the hardware itself. 
		//Of these 48-bits, leftmost 24-bits (6 digits) are associated with the device manufacturer,
		//called as Organizationally Unique Identifier (OUI), 
		//and the rightmost 24-bits represent the identification number of that device
		Random rand = new Random();
		byte[] macAddr = new byte[6];
		rand.nextBytes(macAddr);
		macAddr[0] = (byte)(macAddr[0] & (byte)254);  //zeroing last 2 bytes to make it unicast and locally adminstrated
		StringBuilder sb = new StringBuilder(18);
		for(byte b : macAddr){
			if(sb.length() > 0)
				sb.append(":");
			sb.append(String.format("%02x", b));
		}
		return sb.toString();
	}

	private static String generatePartialRandomMACAddress(String stub){
		Random rand = new Random();
		byte[] macAddr = new byte[3];
		rand.nextBytes(macAddr);
		macAddr[0] = (byte)(macAddr[0] & (byte)254);  //zeroing last 2 bytes to make it unicast and locally adminstrated
		StringBuilder sb = new StringBuilder(18);
		for(byte b : macAddr){
			if(sb.length() > 0)
				sb.append(":");
			sb.append(String.format("%02x", b));
		}
		return stub +":"+sb.toString();
	}

}
