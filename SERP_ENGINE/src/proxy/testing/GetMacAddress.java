package proxy.testing;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Random;

public class GetMacAddress{

	private static int stub_length = 3;
	public static void main(String[] args){

		System.out.println("MAC address : "+getMacAddress());
		System.out.println("Generating full random MAC address");
		for (int i=0 ; i<100; i++){
			System.out.println(generateFullRandomMACAddress());
		}
		System.out.println("Generating partial non organisational random MAC address");
		for (int i=0 ; i<100; i++){
			String organisational_stub = getOrganisationalStub();
			System.out.println(generatePartialRandomMACAddress(organisational_stub));
		}
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