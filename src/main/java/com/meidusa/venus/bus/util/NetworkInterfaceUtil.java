package com.meidusa.venus.bus.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;


public class NetworkInterfaceUtil {

    public static List<String> lookupLocalInterface(){
        List<String> list = new ArrayList<String>();
        
        Enumeration<NetworkInterface> netInterfaces = null;  
        try {  
            netInterfaces = NetworkInterface.getNetworkInterfaces();  
            while (netInterfaces.hasMoreElements()) {
                NetworkInterface ni = netInterfaces.nextElement();  
                Enumeration<InetAddress> ips = ni.getInetAddresses();  
                while (ips.hasMoreElements()) {
                    String ip = ips.nextElement().getHostAddress();
                    if(ip == null || ip.indexOf(":")>=0){
                        continue;
                    }else{
                        InetAddress[] address = InetAddress.getAllByName(ip);
                        if(address != null && address.length >0){
                            list.add(ip);
                        }
                    }
                }
            }  
        } catch (Exception e) {
        }
        return list;
    }
    public static void main(String[] args) {
        lookupLocalInterface();
    }

}
