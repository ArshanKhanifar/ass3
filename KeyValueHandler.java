import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.curator.framework.*;

import org.apache.log4j.*;


public class KeyValueHandler implements KeyValueService.Iface {
    static Logger log;

    private KeyValueService.Client primary_client = null;
    private KeyValueService.Client backup_client = null;
    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private Map<String, String> bucket;
    private static final int BATCH_SIZE = 5;
    private int port;
    private boolean same = true;

    public void update_clients() {
        this.primary_client = null;
        this.backup_client = null;
    }

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
	    this.host = host;
	    this.port = port;
	    this.curClient = curClient;
	    this.zkNode = zkNode;
        this.bucket = new ConcurrentHashMap<String, String>();
	    myMap = new ConcurrentHashMap<String, String>();
        log = Logger.getLogger(KeyValueHandler.class.getName());
        log.setLevel(Level.DEBUG);
    }

    public void debug_get_everything_from_primary(List<String> keys, List<String> values) throws TException {
        same = true;
        for (int i = 0; i < keys.size(); i++) {
            if (!myMap.get(keys.get(i)).equals(values.get(i))) {
                same = false;
            }
        }
        log.debug("Performing check");
    }

    void debug_send_everything_to_backup() throws TException {
        if (Context.type == NodeType.BACKUP || Context.backup_address == null) {
            return;
        }
        List<String> keys = new ArrayList<String>(myMap.keySet());
        List<String> values = new ArrayList<String >(myMap.values());
        KeyValueService.Client client = get_backup_client();
        client.debug_get_everything_from_primary(keys, values);
    }

    class DebugMonitor implements Runnable {
        public void run() {
            int i = 0;
            while (true) {
                log.debug(
                    "Type: " + Context.type + ", " +
                    "Keys: " + myMap.keySet().size() + ", bucket: " + bucket.size() + ", " +
                    "Same: " + same + ", i: " + i
                );
                i += 1;
                //if (i > 100 && Context.type == NodeType.PRIMARY) {
                //    if (i == 101) {
                //        log.debug("Checking if the same!!!!");
                //    }
                //    try {
                //        debug_send_everything_to_backup();
                //    } catch (TException e) {
                //        e.printStackTrace();
                //    }
                //}
                try {
                    Thread.sleep(300);
                } catch (Exception x) {
                    x.printStackTrace();
                }
            }
        }
    }

    public void start_threads() {
        //new Thread(new DebugMonitor()).start();
        new Thread(new SyncThread()).start();
    }

    public String get(String key) throws org.apache.thrift.TException {
	    String ret = myMap.get(key);
	    if (ret == null)
	        return "";
	    else
	        return ret;
    }

    ArrayList<ArrayList<String>> get_batch() {
        ArrayList<String> keys = new ArrayList<>();
        ArrayList<String> values = new ArrayList<>();
        int i = BATCH_SIZE;
        while (!bucket.isEmpty() && i > 0) {
            i--;
            Map.Entry<String,String> entry = bucket.entrySet().iterator().next();
            keys.add(entry.getKey());
            values.add(entry.getValue());
            bucket.remove(entry.getKey());
        }
        ArrayList<ArrayList<String>> batch = new ArrayList<>();
        batch.add(keys);
        batch.add(values);
        return batch;
    }

    class SyncThread implements Runnable {
        public void run() {
            log.debug("Sync thread running");
            try {
                while (true) {
                    Thread.sleep(0);
                    if (Context.type == NodeType.BACKUP || Context.backup_address == null) {
                        continue;
                    }
                    //log.debug("sending to backup");
                    send_batch_to_backup(get_batch());
                }
            } catch (Exception x) {
                x.printStackTrace();
            }
        }
    }
    public void send_batch_to_backup(ArrayList<ArrayList<String>> batch)
        throws TException {
        //log.debug("Batch Size: " + batch.get(0).size());
        if (batch.get(0).size() == 0) {
            return;
        }
        copy_values_to_backup(batch.get(0), batch.get(1));
    }

    public void put(String key, String value) throws TException {
	    myMap.put(key, value);
	    if (Context.type == NodeType.PRIMARY) {
            bucket.put(key, value);
        }
        //update_backup(key, value);
    }

    public void update_backup(String key, String value) throws TException {
        KeyValueService.Client client = get_backup_client();
        client.put(key, value);
    }

    public KeyValueService.Client get_primary_client() throws TException {
        if (primary_client != null) {
            return primary_client;
        }
        primary_client = get_client(Context.primary_address);
        return primary_client;
    }

    public KeyValueService.Client get_backup_client() throws TException {
        if (backup_client != null) {
            return backup_client;
        }
        backup_client = get_client(Context.backup_address);
        return backup_client;
    }

    public KeyValueService.Client get_client(InetSocketAddress address) throws TTransportException {
        TSocket sock = new TSocket(address.getHostName(), address.getPort());
        TTransport transport = new TFramedTransport(sock);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        return new KeyValueService.Client(protocol);
    }

    public void copy_values_to_backup(List<String> keys, List<String> values) throws TTransportException, TException {
        KeyValueService.Client client = get_backup_client();
        client.receive_values_from_primary(keys, values);
    }

    public void copy_everything_to_backup() throws TTransportException, TException {
        List<String> keys = new ArrayList<String>(myMap.keySet());
        List<String> values = new ArrayList<String >(myMap.values());
        copy_values_to_backup(keys, values);
    }

    public void receive_values_from_primary(List<String> keys, List<String> values) throws org.apache.thrift.TException {
        for (int i = 0; i < keys.size(); i++) {
            myMap.put(keys.get(i), values.get(i));
        }
    }
}
