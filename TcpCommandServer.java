import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Locale;

public class TcpCommandServer {
    private static final int DEFAULT_PORT = 5000;
    private static final Set<String> DEFAULT_ROOMS = Collections.unmodifiableSet(
            new LinkedHashSet<>(Arrays.asList("Study", "Game", "TraiNghiem")));
    private static final Map<String, Set<ClientHandler>> rooms = new ConcurrentHashMap<>();
    private static final ExecutorService clientPool = Executors.newCachedThreadPool();

    public static void main(String[] args) {
        int port = args.length > 0 ? parsePort(args[0]) : DEFAULT_PORT;
        initializeDefaultRooms();

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server chat nhóm TCP đang lắng nghe tại cổng " + port);
            while (true) {
                clientPool.execute(new ClientHandler(serverSocket.accept()));
            }
        } catch (IOException e) {
            System.err.println("Không thể khởi động server: " + e.getMessage());
        } finally {
            clientPool.shutdown();
        }
    }

    private static int parsePort(String value) {
        try {
            int port = Integer.parseInt(value);
            if (port < 1 || port > 65535) throw new NumberFormatException();
            return port;
        } catch (NumberFormatException e) {
            System.err.println("Cổng không hợp lệ, sử dụng cổng mặc định " + DEFAULT_PORT);
            return DEFAULT_PORT;
        }
    }

    private static String roomList() {
        String[] roomNames = rooms.keySet().toArray(new String[0]);
        Arrays.sort(roomNames);
        StringBuilder result = new StringBuilder();
        for (String roomName : roomNames) {
            if (result.length() > 0) result.append(", ");
            result.append(roomName);
        }
        return result.toString();
    }

    static void serve(Socket socket) {
        initializeDefaultRooms();
        new ClientHandler(socket).run();
    }

    private static void initializeDefaultRooms() {
        DEFAULT_ROOMS.forEach(room -> rooms.putIfAbsent(room, ConcurrentHashMap.newKeySet()));
    }

    static String process(String request) {
        String trimmed = request.replace("\uFEFF", "").trim();
        if (trimmed.equalsIgnoreCase("PING")) {
            return "OK PONG";
        }
        if (trimmed.equalsIgnoreCase("TIME")) {
            return "OK " + LocalDateTime.now();
        }
        if (trimmed.equalsIgnoreCase("QUIT")) {
            return "OK BYE";
        }
        if (trimmed.regionMatches(true, 0, "UPPER ", 0, 6)) {
            return "OK " + trimmed.substring(6).toUpperCase(Locale.ROOT);
        }
        return "ERR UNKNOWN_COMMAND";
    }

    private static boolean isCommand(String request) {
        String trimmed = request.replace("\uFEFF", "").trim();
        return trimmed.equalsIgnoreCase("PING")
                || trimmed.equalsIgnoreCase("TIME")
                || trimmed.equalsIgnoreCase("QUIT")
                || trimmed.regionMatches(true, 0, "UPPER ", 0, 6);
    }

    private static String getOrCreateRoom(String roomName) {
        String normalized = roomName.trim();
        if (normalized.isEmpty()) return null;
        rooms.putIfAbsent(normalized, ConcurrentHashMap.newKeySet());
        return normalized;
    }

    private static void broadcast(String room, String message, ClientHandler excluded) {
        Set<ClientHandler> members = rooms.get(room);
        if (members != null) {
            for (ClientHandler member : members) {
                if (member != excluded) member.send(message);
            }
        }
    }

    private static final class ClientHandler implements Runnable {
        private final Socket socket;
        private PrintWriter out;
        private String name;
        private String currentRoom;

        private ClientHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            String remote = String.valueOf(socket.getRemoteSocketAddress());
            try (Socket ignored = socket;
                 BufferedReader in = new BufferedReader(new InputStreamReader(
                         socket.getInputStream(), StandardCharsets.UTF_8))) {
                out = new PrintWriter(new OutputStreamWriter(
                        socket.getOutputStream(), StandardCharsets.UTF_8), true);
                send("=== CHAT NHOM TCP ===");
                send("Nhập tên của bạn:");

                name = in.readLine();
                if (name == null || name.trim().isEmpty()) {
                    send("Tên không được để trống.");
                    return;
                }
                name = name.trim();
                send("Xin chào " + name + "!");

                while (true) {
                    if (currentRoom == null) {
                        send("Danh sách phòng: " + roomList());
                        send("Chọn phòng (có thể nhập tên phòng mới):");
                    }

                    String input = in.readLine();
                    if (input == null) break;
                    input = input.trim();

                    if ("/exit".equalsIgnoreCase(input)) {
                        send("Đã ngắt kết nối. Tạm biệt!");
                        break;
                    }
                    if (isCommand(input)) {
                        send(process(input));
                        if ("QUIT".equalsIgnoreCase(input)) break;
                        continue;
                    }
                    if (currentRoom == null) {
                        joinRoom(input);
                    } else if ("/quit".equalsIgnoreCase(input)) {
                        leaveRoom();
                    } else if (!input.isEmpty()) {
                        broadcast(currentRoom, name + ": " + input, this);
                    }
                }
            } catch (IOException e) {
                System.err.println("Lỗi client " + remote + ": " + e.getMessage());
            } finally {
                leaveRoom();
                System.out.println("Đã ngắt kết nối: " + remote);
            }
        }

        private void joinRoom(String requestedRoom) {
            String room = getOrCreateRoom(requestedRoom);
            if (room == null) {
                send("Tên phòng không được để trống.");
                return;
            }
            currentRoom = room;
            rooms.get(room).add(this);
            send("Đã vào phòng: " + room);
            send("Nhập tin nhắn, /quit để đổi phòng, /exit để thoát.");
            broadcast(room, "*** " + name + " đã vào phòng ***", this);
        }

        private void leaveRoom() {
            if (currentRoom == null) return;
            String room = currentRoom;
            Set<ClientHandler> members = rooms.get(room);
            if (members != null) {
                members.remove(this);
                broadcast(room, "*** " + name + " đã rời phòng ***", this);
            }
            currentRoom = null;
        }

        private synchronized void send(String message) {
            if (out != null) out.println(message);
        }
    }
}
