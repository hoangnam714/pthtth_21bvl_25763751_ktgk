import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class TcpCommandClient {
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 5000;

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : DEFAULT_HOST;
        int port = args.length > 1 ? parsePort(args[1]) : DEFAULT_PORT;

        try (Socket socket = new Socket(host, port);
             BufferedReader console = new BufferedReader(new InputStreamReader(
                     System.in, StandardCharsets.UTF_8));
             BufferedReader in = new BufferedReader(new InputStreamReader(
                     socket.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(
                     socket.getOutputStream(), StandardCharsets.UTF_8), true)) {
            System.out.println("Nhập tên, xem danh sách phòng hiện có, chọn phòng, hoặc dùng PING, TIME, UPPER text, QUIT.");
            Thread receiver = new Thread(() -> receiveMessages(in), "luong-nhan-tin");
            receiver.setDaemon(true);
            receiver.start();

            String input;
            while ((input = console.readLine()) != null) {
                out.println(input);
                String trimmed = input.trim();
                if ("/exit".equalsIgnoreCase(trimmed)
                        || "QUIT".equalsIgnoreCase(trimmed)) {
                    break;
                }
            }
            receiver.join(1000);
        } catch (NumberFormatException e) {
            System.err.println("Cổng phải là số nguyên từ 1 đến 65535.");
        } catch (IOException e) {
            System.err.println("Lỗi kết nối tới server: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Client bị gián đoạn.");
        }
    }

    private static void receiveMessages(BufferedReader in) {
        try {
            String message;
            while ((message = in.readLine()) != null) {
                System.out.println(message);
            if (message.startsWith("Đã ngắt kết nối.")) break;
            }
        } catch (IOException e) {
            System.err.println("Mất kết nối tới server.");
        }
    }

    private static int parsePort(String value) {
        int port = Integer.parseInt(value);
        if (port < 1 || port > 65535) throw new NumberFormatException();
        return port;
    }
}
