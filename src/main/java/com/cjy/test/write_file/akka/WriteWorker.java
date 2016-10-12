package com.cjy.test.write_file.akka;

import akka.actor.UntypedActor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static com.cjy.test.write_file.WriteFileTest.LINE;
import static com.cjy.test.write_file.WriteFileTest.dir;
import static com.cjy.test.write_file.WriteFileTest.lineCount;
import static com.cjy.test.write_file.WriteFileTest.threadCount;

public class WriteWorker extends UntypedActor {
    private int index;

    public WriteWorker(int index) {
        this.index = index;
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        getSelf().tell(new StartWriteMessage(), null);
    }

    public void onReceive(Object message) throws Exception {
        if (message instanceof StartWriteMessage) {
            int bufferSize = 8 * 1024 * 1024;
            int lineLength = LINE.getBytes().length;
            long totalLength = 0;
            try (FileWriter bioFileWriter = new FileWriter(new File(dir + "file_akka_" + index + ".txt"));
                    BufferedWriter bufferedWriter = new BufferedWriter(bioFileWriter, bufferSize)) {
                for (int i = 0; i < lineCount / threadCount; i++) {
                    bufferedWriter.write(LINE);
                    totalLength += lineLength;
                }
                bufferedWriter.flush();
            }
            catch (IOException e) {
                e.printStackTrace();
            }

            getContext().parent().tell(new FinishWriteMessage(index, totalLength), null);
        }
    }
}
