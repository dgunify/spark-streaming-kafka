package com.dgunify;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;

/**
 * @author dgunify
 *
 */
public class KafkaUtil {
	public static Set<String> getAllGroupsForTopic(Properties p) {
        AdminClient client = AdminClient.create(p);
        try {
        	ListTopicsResult l = client.listTopics();
        	KafkaFuture<Set<String>> ks = l.names();
        	
            return ks.get();
        }catch (Exception e) {
        	e.printStackTrace();
		} finally {
            client.close();
        }
		return null;
    }
	
	public static List<String> getAllGroupsForTopic(Properties p,String cons) {
        AdminClient client = AdminClient.create(p);
        try {
        	ListTopicsResult l = client.listTopics();
        	KafkaFuture<Set<String>> ks = l.names();
        	Set<String> st = ks.get();
        	List<String> reSet = new ArrayList<String>();
        	for(String k : st) {
        		if(k.contains(cons)) {
        			reSet.add(k);
        		}
        	}
            return reSet;
        }catch (Exception e) {
        	e.printStackTrace();
		} finally {
            client.close();
        }
		return null;
    }
}
