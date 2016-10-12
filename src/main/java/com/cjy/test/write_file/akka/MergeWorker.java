package com.cjy.test.write_file.akka;

import akka.actor.UntypedActor;

import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static com.cjy.test.write_file.WriteFileTest.dir;

public class MergeWorker extends UntypedActor {
    private int index;
    private int mergeStart;

    public MergeWorker(int index, int mergeStart) {
        this.index = index;
        this.mergeStart = mergeStart;
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        getSelf().tell(new StartMergeMessage(), null);
    }

    public void onReceive(Object message) throws Exception {
        if (message instanceof StartMergeMessage) {
            int bufferSize = 8 * 1024 * 1024;
            try (FileChannel inFile = FileChannel.open(Paths.get(dir + "file_akka_" + index + ".txt"), StandardOpenOption.READ);
                    FileChannel outFile = FileChannel.open(Paths.get(dir + "final_akka.txt"), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                long size = inFile.size();
                outFile.position(mergeStart);
                long pos = 0;
                long count;
                while (pos < size) {
                    count = size - pos > bufferSize ? bufferSize : size - pos;
                    pos += inFile.transferTo(pos, count, outFile);
                }
            }
            getContext().parent().tell(new FinishMergeMessage(), null);
        }
    }
}
