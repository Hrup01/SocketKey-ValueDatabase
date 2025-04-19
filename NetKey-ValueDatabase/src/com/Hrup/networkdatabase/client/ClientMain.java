package com.Hrup.networkdatabase.client;
import com.Hrup.networkdatabase.server.ServerMain;

import java.io.*;
import java.net.Socket;
import java.util.Scanner;


public class ClientMain {
    public static void main(String[] args) {
        try (Socket socket = new Socket("localhost", ServerMain.getPort());
             BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()),8192);
             PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);//字符打印流把命令从通道传输到服务器
             Scanner sc = new Scanner(System.in)) {
            System.out.print("127.0.0.1:" + ServerMain.getPort() + "> ");
            while (sc.hasNextLine()) {
                //传输命令
                String command = sc.nextLine();
                pw.println(command);
                //获取服务器返回的多行响应
                String response;
                while ((response = br.readLine()) != null && !response.isEmpty()) {
                    System.out.println(response);
                }
                System.out.print("127.0.0.1:" + ServerMain.getPort() + "> ");
            }
        } catch (IOException e) {
            System.err.println("Error connecting to server: " + e.getMessage());
        }
    }
}
