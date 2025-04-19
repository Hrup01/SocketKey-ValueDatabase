package com.Hrup.networkdatabase.server;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
// 服务端主类
public class ServerMain {
    // 存储字符串类型的 key - value 数据
    private static final Map<String, String> stringStore = new HashMap<>();
    // 存储双向链表类型的 key - value 数据
    private static final Map<String, LinkedList<String>> listStore = new HashMap<>();
    // 存储哈希类型的 key - value 数据
    private static final Map<String, Map<String, String>> hashStore = new HashMap<>();
    // 线程池，用于处理多个客户端连接
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(10);
    // 日志文件
    private static final File logFile;
    // 数据持久化文件
    private static final File dataFile;
    // 监听端口号
    private static int port;

    public static int getPort() {
        return port;
    }

    static {
        // 加载配置文件
        Properties properties = new Properties();
        try (InputStream input = new FileInputStream("NetKey-ValueDatabase\\src\\com\\Hrup\\networkdatabase\\config.properties")) {
            properties.load(input);
            port = Integer.parseInt(properties.getProperty("server.port"));
            logFile = new File(properties.getProperty("log.file.path"));
            dataFile = new File(properties.getProperty("data.persist.file.path"));
            // 加载持久化数据
            loadData(dataFile);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load configuration", e);
        }
    }

    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            log("Server started on port " + port);
            while (true) {
                // 接受客户端连接
                Socket clientSocket = serverSocket.accept();
                log("New client connected: " + clientSocket.getInetAddress());
                // 提交任务到线程池处理客户端请求
                threadPool.submit(new ClientHandler(clientSocket));
            }
        } catch (IOException e) {
            logError("Error accepting client connection: " + e.getMessage());
        } finally {
            // 关闭线程池
            threadPool.shutdown();
        }
    }

    // 加载持久化数据
    private static void loadData(File file) {
        if (file.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(" ");
                    if (parts[0].equals("set")) {
                        stringStore.put(parts[1], parts[2]);
                    } else if (parts[0].equals("del")) {
                        stringStore.remove(parts[1]);
                    }
                }
            } catch (IOException e) {
                logError("Error loading data: " + e.getMessage());
            }
        }
    }

    // 记录日志
    private static void log(String message) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFile, true))) {
            writer.write(new Date() + " - " + message);
            writer.newLine();
        } catch (IOException e) {
            System.err.println("Error writing to log file: " + e.getMessage());
        }
    }

    // 记录错误日志
    private static void logError(String message) {
        log("ERROR: " + message);
    }

    // 客户端处理类
    static class ClientHandler implements Runnable {
        private final Socket clientSocket;

        public ClientHandler(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {
                String inputLine;
                //用字符输入流读取客户端发送到通信通道中的命令
                //用switch语句针对不同的命令执行不同的要求
                while ((inputLine = in.readLine()) != null) {
                    String[] parts = inputLine.split(" ");//为了能够让help命令查询到单独的命令
                    String command = parts[0];
                    String response;
                    try {
                        switch (command) {
                            case "set":
                                response = handleSet(parts);
                                break;
                            case "get":
                                response = handleGet(parts);
                                break;
                            case "del":
                                response = handleDel(parts);
                                break;
                            case "lpush":
                                response = handleLPush(parts);
                                break;
                            case "rpush":
                                response = handleRPush(parts);
                                break;
                            case "range":
                                response = handleRange(parts);
                                break;
                            case "len":
                                response = handleLen(parts);
                                break;
                            case "lpop":
                                response = handleLPop(parts);
                                break;
                            case "rpop":
                                response = handleRPop(parts);
                                break;
                            case "ldel":
                                response = handleLDel(parts);
                                break;
                            case "ping":
                                response = "pong";
                                break;
                            case "help":
                                if (parts.length > 1) {
                                    response = handleHelpSingle(parts[1]);
                                } else {
                                    response = handleHelpAll();
                                }
                                break;
                            case "hset":
                                response = handleHSet(parts);
                                break;
                            case "hget":
                                response = handleHGet(parts);
                                break;
                            case "hdel":
                                response = handleHDel(parts);
                                break;
                            default:
                                response = "Unknown command";
                        }
                    } catch (Exception e) {
                        response = "Error: " + e.getMessage();
                        logError("Client " + clientSocket.getInetAddress() + " - " + e.getMessage());
                    }
                    //用打印流返回相应命令执行的结果
                    out.println(response);
                    out.println();//发送一个空行表示相应结束
                }
            } catch (IOException e) {
                logError("Error handling client: " + clientSocket.getInetAddress() + " - " + e.getMessage());
            } finally {
                try {
                    clientSocket.close();
                    log("Client disconnected: " + clientSocket.getInetAddress());
                } catch (IOException e) {
                    logError("Error closing client socket: " + e.getMessage());
                }
            }
        }

        // 处理 set 指令
        private String handleSet(String[] parts) {
            if (parts.length != 3) {
                return "Usage: set [key] [value]";
            }
            String key = parts[1];
            String value = parts[2];
            stringStore.put(key, value);
            persistData("set " + key + " " + value);
            return "OK";
        }

        // 处理 get 指令
        private String handleGet(String[] parts) {
            if (parts.length != 2) {
                return "Usage: get [key]";
            }
            String key = parts[1];
            return stringStore.getOrDefault(key, "null");
        }

        // 处理 del 指令
        private String handleDel(String[] parts) {
            if (parts.length != 2) {
                return "Usage: del [key]";
            }
            String key = parts[1];
            if (stringStore.remove(key) != null) {
                persistData("del " + key);
                return "OK";
            }
            return "Key not found";
        }

        // 处理 lpush 指令
        private String handleLPush(String[] parts) {
            if (parts.length != 3) {
                return "Usage: lpush [key] [value]";
            }
            String key = parts[1];
            String value = parts[2];
            listStore.computeIfAbsent(key, k -> new LinkedList<>()).addFirst(value);
            return "OK";
        }

        // 处理 rpush 指令
        private String handleRPush(String[] parts) {
            if (parts.length != 3) {
                return "Usage: rpush [key] [value]";
            }
            String key = parts[1];
            String value = parts[2];
            listStore.computeIfAbsent(key, k -> new LinkedList<>()).addLast(value);
            return "OK";
        }

        // 处理 range 指令
        private String handleRange(String[] parts) {
            if (parts.length != 4) {
                return "Usage: range [key] [start] [end]";
            }
            String key = parts[1];
            int start = Integer.parseInt(parts[2]);
            int end = Integer.parseInt(parts[3]);
            LinkedList<String> list = listStore.get(key);
            if (list == null) {
                return "Key not found";
            }
            if (start < 0 || start >= list.size() || end < start || end >= list.size()) {
                return "Invalid range";
            }
            StringBuilder result = new StringBuilder();
            for (int i = start; i <= end; i++) {
                result.append(list.get(i)).append(" ");
            }
            return result.toString().trim();
        }

        // 处理 len 指令
        private String handleLen(String[] parts) {
            if (parts.length != 2) {
                return "Usage: len [key]";
            }
            String key = parts[1];
            LinkedList<String> list = listStore.get(key);
            return list == null ? "0" : String.valueOf(list.size());
        }

        // 处理 lpop 指令
        private String handleLPop(String[] parts) {
            if (parts.length != 2) {
                return "Usage: lpop [key]";
            }
            String key = parts[1];
            LinkedList<String> list = listStore.get(key);
            if (list == null || list.isEmpty()) {
                return "null";
            }
            return list.removeFirst();
        }

        // 处理 rpop 指令
        private String handleRPop(String[] parts) {
            if (parts.length != 2) {
                return "Usage: rpop [key]";
            }
            String key = parts[1];
            LinkedList<String> list = listStore.get(key);
            if (list == null || list.isEmpty()) {
                return "null";
            }
            return list.removeLast();
        }

        // 处理 ldel 指令
        private String handleLDel(String[] parts) {
            if (parts.length != 2) {
                return "Usage: ldel [key]";
            }
            String key = parts[1];
            if (listStore.remove(key) != null) {
                return "OK";
            }
            return "Key not found";
        }

        // 处理 help 指令（获取所有指令）
        private String handleHelpAll() {
            String helpInfo = "Available commands:\n" +
                    "set [key] [value]\n" +
                    "get [key]\n" +
                    "del [key]\n" +
                    "lpush [key] [value]\n" +
                    "rpush [key] [value]\n" +
                    "range [key] [start] [end]\n" +
                    "len [key]\n" +
                    "lpop [key]\n" +
                    "rpop [key]\n" +
                    "ldel [key]\n" +
                    "ping\n" +
                    "help\n" +
                    "help [command]\n" +
                    "hset [key] [field] [value]\n" +
                    "hget [key] [field]\n" +
                    "hdel [key] [field] or hdel [key]";
            //System.out.println("Generated help all info: " + helpInfo); 测试语句，测试是否能够返回所有命令
            return helpInfo;
        }

        // 处理 help 指令（获取单个指令）
        //根据重写的run方法中的spilt把help xxx分成两部分 然后根据[1]索引找到要查找的命令
        private String handleHelpSingle(String command) {
            switch (command) {
                case "set":
                    return "Usage: set [key] [value] - Store a key - value pair";
                case "get":
                    return "Usage: get [key] - Retrieve the value associated with a key";
                case "del":
                    return "Usage: del [key] - Delete a key - value pair";
                case "lpush":
                    return "Usage: lpush [key] [value] - Push a value to the left of a list";
                case "rpush":
                    return "Usage: rpush [key] [value] - Push a value to the right of a list";
                case "range":
                    return "Usage: range [key] [start] [end] - Get a range of values from a list";
                case "len":
                    return "Usage: len [key] - Get the length of a list";
                case "lpop":
                    return "Usage: lpop [key] - Pop a value from the left of a list";
                case "rpop":
                    return "Usage: rpop [key] - Pop a value from the right of a list";
                case "ldel":
                    return "Usage: ldel [key] - Delete a list";
                case "ping":
                    return "Usage: ping - Send a heartbeat request";
                case "help":
                    return "Usage: help or help [command] - Get help information";
                case "hset":
                    return "Usage: hset [key] [field] [value] - Store a field - value pair in a hash";
                case "hget":
                    return "Usage: hget [key] [field] - Retrieve the value of a field in a hash";
                case "hdel":
                    return "Usage: hdel [key] [field] or hdel [key] - Delete a field - value pair or a whole hash";
                default:
                    return "Unknown command: " + command;
            }
        }

        // 处理 hset 指令
        private String handleHSet(String[] parts) {
            if (parts.length != 4) {
                return "Usage: hset [key] [field] [value]";
            }
            String key = parts[1];
            String field = parts[2];
            String value = parts[3];
            hashStore.computeIfAbsent(key, k -> new HashMap<>()).put(field, value);
            return "OK";
        }

        // 处理 hget 指令
        private String handleHGet(String[] parts) {
            if (parts.length != 3) {
                return "Usage: hget [key] [field]";
            }
            String key = parts[1];
            String field = parts[2];
            Map<String, String> hash = hashStore.get(key);
            return hash == null ? "null" : hash.getOrDefault(field, "null");
        }

        // 处理 hdel 指令
        private String handleHDel(String[] parts) {
            if (parts.length == 2) {
                String key = parts[1];
                if (hashStore.remove(key) != null) {
                    return "OK";
                }
                return "Key not found";
            } else if (parts.length == 3) {
                String key = parts[1];
                String field = parts[2];
                Map<String, String> hash = hashStore.get(key);
                if (hash != null && hash.remove(field) != null) {
                    return "OK";
                }
                return "Key or field not found";
            }
            return "Usage: hdel [key] [field] or hdel [key]";
        }

        // 持久化数据
        private void persistData(String data) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(dataFile, true))) {
                writer.write(data);
                writer.newLine();
            } catch (IOException e) {
                logError("Error persisting data: " + e.getMessage());
            }
        }
    }
}
