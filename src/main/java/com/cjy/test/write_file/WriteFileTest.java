package com.cjy.test.write_file;

import akka.actor.ActorSystem;
import akka.actor.Props;
import com.cjy.test.write_file.akka.AkkaAgent;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;

/**
 * 原始测试数据的生成
 */
public class WriteFileTest {
    public static final String LINE = "abcdefg123456\n";
    public static long lineCount = 10000000; // 文件行数
    public static int threadCount = 8; // 线程数
    public static String dir; // 目录

    public static void main(String[] args) throws IOException, InterruptedException {
        //        // 10组测试
        //        for (int i = 0; i < 10; i++) {
        //            System.out.println("第" + i + "组");
        //            System.out.println("当前line count:" + lineCount);
        //            // 每组测试做3遍取平均值
        //            for (int k = 0; k < 3; k++) {

        lineCount = Integer.parseInt(args[0]) * lineCount;
        System.out.println("当前行数:" + lineCount);
        mkdir(args[0]);

        // BIO
        writeByBIO();

        // Normal NIO
        // Stream Channel NIO
        writeByStreamChannelNIO();

        // Random Access File Channel NIO
        writeByRandomAccessFileChannelNIO();

        // Mapped Memory NIO
        writeByMappedMemory();

        // 多线程 + Normal NIO
        writeByChannelWithMultiThreads();

        // 多线程 + Mapped Memory NIO
        writeByMappedMemoryWithMultiThreads();

        // Merge
        writeByMerge();

        // Akka test
        writeByAkka();
        //            }
        //
        //            lineCount += lineCount;
        //            System.out.println();
        //        }
    }

    public static void mkdir(String i) {
        //        dir = "test_data/" + i + "_" + k + "/";
        dir = "test_data/" + i + "/";
        File file = new File(dir);
        file.mkdirs();
    }

    public static void mkdir() {
        dir = "test_data/" + System.currentTimeMillis() + "/";
        File file = new File(dir);
        file.mkdirs();
    }

    public static void writeByBIO() throws IOException {
        long start = System.currentTimeMillis();
        File file = new File(dir + "bio.txt");
        FileWriter bioFileWriter = new FileWriter(file);
        BufferedWriter bufferedWriter = new BufferedWriter(bioFileWriter, 8192); // 8k
        for (long i = 0; i < lineCount; i++) {
            bufferedWriter.write(LINE);
        }
        bufferedWriter.flush();
        bufferedWriter.close();
        long end = System.currentTimeMillis();
        System.out.println("bio:" + (end - start));
    }

    public static void writeByStreamChannelNIO() throws IOException {
        long start = System.currentTimeMillis();
        File file = new File(dir + "stream_channel_nio.txt");
        FileOutputStream fos = new FileOutputStream(file);
        FileChannel fileChannel = fos.getChannel();
        byte[] bytes = LINE.getBytes();
        ByteBuffer buf = ByteBuffer.allocate(bytes.length);
        for (long i = 0; i < lineCount; i++) {
            buf.clear();
            buf.put(bytes);
            buf.flip();
            while (buf.hasRemaining()) {
                fileChannel.write(buf);
            }
        }
        fileChannel.close();
        fos.close();
        long end = System.currentTimeMillis();
        System.out.println("stream_channel_nio:" + (end - start));
    }

    public static void writeByRandomAccessFileChannelNIO() throws IOException {
        long start = System.currentTimeMillis();
        RandomAccessFile file = new RandomAccessFile(dir + "random_access_file_nio.txt", "rw");
        FileChannel fileChannel = file.getChannel();
        byte[] bytes = LINE.getBytes();
        ByteBuffer buf = ByteBuffer.allocate(bytes.length);
        for (long i = 0; i < lineCount; i++) {
            buf.clear();
            buf.put(bytes);
            buf.flip();
            while (buf.hasRemaining()) {
                fileChannel.write(buf);
            }
        }
        fileChannel.close();
        file.close();
        long end = System.currentTimeMillis();
        System.out.println("random_access_file_nio:" + (end - start));
    }

    public static void writeByMappedMemory() throws IOException {
        long start = System.currentTimeMillis();
        byte[] buffer = LINE.getBytes();
        RandomAccessFile file = new RandomAccessFile(dir + "mapped_memory.txt", "rw");
        FileChannel rwChannel = file.getChannel();
        ByteBuffer wrBuf = rwChannel.map(FileChannel.MapMode.READ_WRITE, 0, buffer.length * lineCount);
        for (int i = 0; i < lineCount; i++) {
            wrBuf.put(buffer);
        }
        rwChannel.close();
        file.close();
        long end = System.currentTimeMillis();
        System.out.println("mapped_memory:" + (end - start));
    }

    public static void writeByChannelWithMultiThreads() throws InterruptedException, IOException {
        long start = System.currentTimeMillis();
        final CountDownLatch latch = new CountDownLatch(threadCount);

        for (int k = 0; k < threadCount; k++) {
            final long position = lineCount / threadCount * k * LINE.getBytes().length;
            Thread t = new Thread() {
                @Override
                public void run() {
                    try {
                        RandomAccessFile file = new RandomAccessFile(dir + "channel_with_multi_threads.txt", "rw");
                        file.seek(position);
                        FileChannel fileChannel = file.getChannel();
                        byte[] bytes = LINE.getBytes();
                        ByteBuffer buf = ByteBuffer.allocate(bytes.length);
                        for (long i = 0; i < lineCount / threadCount; i++) {
                            buf.clear();
                            buf.put(bytes);
                            buf.flip();
                            while (buf.hasRemaining()) {
                                fileChannel.write(buf);
                            }
                        }
                        fileChannel.close();
                        file.close();
                        latch.countDown();
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                        // skip
                    }
                }
            };
            t.start();
        }

        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("channel_with_multi_threads:" + (end - start));
    }

    public static void writeByMappedMemoryWithMultiThreads() throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        final CountDownLatch latch = new CountDownLatch(threadCount);

        for (int k = 0; k < threadCount; k++) {
            final long position = lineCount / threadCount * k * LINE.getBytes().length;
            Thread t = new Thread() {
                @Override
                public void run() {
                    try (RandomAccessFile file = new RandomAccessFile(dir + "mapped_memory_with_multi_threads.txt", "rw");
                            FileChannel rwChannel = file.getChannel()) {
                        byte[] bytes = LINE.getBytes();
                        ByteBuffer wrBuf = rwChannel.map(FileChannel.MapMode.READ_WRITE, position, bytes.length * lineCount / threadCount);
                        for (int i = 0; i < lineCount / threadCount; i++) {
                            wrBuf.put(bytes);
                        }
                        latch.countDown();
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            };
            t.start();
        }

        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("mapped_memory_with_multi_threads:" + (end - start));
    }

    public static void writeByMerge() throws InterruptedException, IOException {
        long start = System.currentTimeMillis();

        final CountDownLatch latch = new CountDownLatch(threadCount);
        final int bufferSize = 8 * 1024 * 1024;

        for (int k = 0; k < threadCount; k++) {
            final int finalK = k;
            Thread t = new Thread() {
                @Override
                public void run() {
                    try (FileWriter bioFileWriter = new FileWriter(new File(dir + "file_" + finalK + ".txt"));
                            BufferedWriter bufferedWriter = new BufferedWriter(bioFileWriter, bufferSize)) {
                        for (int i = 0; i < lineCount / threadCount; i++) {
                            bufferedWriter.write(LINE);
                        }
                        bufferedWriter.flush();
                        latch.countDown();
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            };
            t.start();
        }

        latch.await();

        // merge files
        long pos = 0;
        final CountDownLatch countDownLatch = new CountDownLatch(threadCount);

        for (int k = 0; k < threadCount; k++) {
            final FileChannel inFile = FileChannel.open(Paths.get(dir + "file_" + k + ".txt"), StandardOpenOption.READ);
            final FileChannel outFile = FileChannel.open(Paths.get(dir + "final.txt"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            final long finalPos = pos;
            Thread t = new Thread() {
                @Override
                public void run() {
                    try {
                        long size = inFile.size();
                        outFile.position(finalPos);
                        long pos = 0;
                        long count;
                        while (pos < size) {
                            count = size - pos > bufferSize ? bufferSize : size - pos;
                            pos += inFile.transferTo(pos, count, outFile);
                        }
                        inFile.close();
                        outFile.close();
                        countDownLatch.countDown();
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            };

            pos += inFile.size();
            t.start();
        }

        countDownLatch.await();

        long end = System.currentTimeMillis();
        System.out.println("merge:" + (end - start));
    }

    public static void writeByAkka() {
        long start = System.currentTimeMillis();
        ActorSystem system = ActorSystem.create("huge-file-generator");
        system.actorOf(Props.create(AkkaAgent.class, start), "actor-agent");
    }
}