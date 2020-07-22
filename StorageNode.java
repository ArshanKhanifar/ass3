import java.net.InetSocketAddress;
import java.util.*;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;

import org.apache.log4j.*;

enum NodeType {
    PRIMARY,
    BACKUP
}

class Context {
    static String parent_znode_path;
    static String node_address;
    static NodeType type;
    static InetSocketAddress primary_address;
    static InetSocketAddress backup_address;
    static KeyValueHandler handler;
}

class ZNodeManager {
    static Logger log;
    private CuratorFramework _client;

    ZNodeManager(CuratorFramework client) {
        log = Logger.getLogger(ZNodeManager.class.getName());
        log.setLevel(Level.DEBUG);
        this._client = client;
    }

    void create_node() throws Exception {
        if (get_children().size() >= 2) {
            throw new RuntimeException("Can't have more than two nodes");
        }
        this._client.create()
            .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
            .forPath(String.format("%s/", Context.parent_znode_path),
                    Context.node_address.getBytes());
    }

    List<String> get_children() throws Exception {
        List<String> children = this._client.getChildren()
                .forPath(Context.parent_znode_path);
        Collections.sort(children);
        return children;
    }

    List<String> get_children_and_watch(CuratorWatcher watcher) throws Exception {
        List<String> children = this._client.getChildren().usingWatcher(watcher)
            .forPath(Context.parent_znode_path);
        Collections.sort(children);
        return children;
    }

    void become_primary_if_one_node_exists() throws Exception {
        Context.handler.update_clients();
        List<String> children = get_children();
        Context.primary_address = _get_address(children.get(0));
        if (children.size() == 1) {
            Context.type = NodeType.PRIMARY;
            Context.backup_address = null;
            return;
        }
        Context.backup_address = _get_address(children.get(1));
        if (Context.type == NodeType.PRIMARY) {
            Context.handler.copy_everything_to_backup();
            return;
        }
        Context.type = NodeType.BACKUP;
    }

    InetSocketAddress _get_address(String child) throws Exception {
        byte[] data = this._client.getData().forPath(Context.parent_znode_path + "/" + child);
        String strData = new String(data);
        String[] primary = strData.split(":");
        return new InetSocketAddress(primary[0], Integer.parseInt(primary[1]));
    }
}


class StorageWatcher implements CuratorWatcher {
    static Logger log;
    private ZNodeManager _node_manager;

    StorageWatcher(ZNodeManager znode_manager) {
        log = Logger.getLogger(StorageWatcher.class.getName());
        this._node_manager = znode_manager;
    }

    synchronized public void process(WatchedEvent watchedEvent) throws Exception {
        log.info("ZooKeeper event: " + watchedEvent);
        List<String> children = this._node_manager.get_children_and_watch(this);
        log.info("Children now: " + children.size());
        this._node_manager.become_primary_if_one_node_exists();
    }
}


public class StorageNode {
    static Logger log;

    public static void _write_context(String[] args, KeyValueHandler handler) {
        Context.parent_znode_path = args[3];
        Context.node_address = String.format("%s:%s", args[0], args[1]);
        Context.handler = handler;
    }

    public static void main(String [] args) throws Exception {
	    BasicConfigurator.configure();
	    log = Logger.getLogger(StorageNode.class.getName());
	    log.setLevel(Level.DEBUG);

	    if (args.length != 4) {
	        System.err.println("Usage: java StorageNode host port zkconnectstring zknode");
	        System.exit(-1);
	    }

	    CuratorFramework curClient =
	        CuratorFrameworkFactory.builder()
	        .connectString(args[2])
	        .retryPolicy(new RetryNTimes(10, 1000))
	        .connectionTimeoutMs(1000)
	        .sessionTimeoutMs(10000)
	        .build();

	    curClient.start();
	    Runtime.getRuntime().addShutdownHook(new Thread() {
	    	public void run() {
	    	    curClient.close();
	    	}
	    });

	    KeyValueHandler key_value_handler = new KeyValueHandler(args[0], Integer.parseInt(args[1]), curClient, args[3]);
	    KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(key_value_handler);
	    TServerSocket socket = new TServerSocket(Integer.parseInt(args[1]));
	    TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
	    sargs.protocolFactory(new TBinaryProtocol.Factory());
	    sargs.transportFactory(new TFramedTransport.Factory());
	    sargs.processorFactory(new TProcessorFactory(processor));
	    sargs.maxWorkerThreads(64);
	    TServer server = new TThreadPoolServer(sargs);
	    log.info("Launching server");

	    _write_context(args, key_value_handler);

	    new Thread(new Runnable() {
	    	public void run() {
	    	    server.serve();
	    	}
	    }).start();

        String node_address = String.format("%s:%s", args[0], args[1]);
        log.debug("NODE ADDRESS: " + node_address);

        String parent_znode_path = args[3];
        log.debug("parent znode path: " + parent_znode_path);

        // creating an ephemeral node
        ZNodeManager node_manager = new ZNodeManager(curClient);
        node_manager.create_node();

        // setting up a watcher:
        StorageWatcher watcher = new StorageWatcher(node_manager);
        node_manager.get_children_and_watch(watcher);
        node_manager.become_primary_if_one_node_exists();
        key_value_handler.start_threads();
    }
}
