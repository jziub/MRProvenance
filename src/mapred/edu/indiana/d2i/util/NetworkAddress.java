package edu.indiana.d2i.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.Enumeration;

public class NetworkAddress {	
	public static String getIP(String intefName) {
		String ipReg = "(/[01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])";
		NetworkInterface netint;
		try {
			netint = NetworkInterface.getByName(intefName);
			if (netint != null) {
				Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
				for (InetAddress inetAddress : Collections.list(inetAddresses)) {
					if (inetAddress.toString().matches(ipReg)) {
						return inetAddress.toString().substring(1);
					}
				}
			}

		} catch (SocketException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	/* test driver */
//	public static void main(String[] args) {
//		NetworkAddress net = new NetworkAddress();
//		System.out.println(net.getIP());
//	}
}
