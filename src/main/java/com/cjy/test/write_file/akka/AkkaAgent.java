package com.cjy.test.write_file.akka;

import akka.actor.Props;
import akka.actor.UntypedActor;

import static com.cjy.test.write_file.WriteFileTest.threadCount;

public class AkkaAgent extends UntypedActor {
    private long start;
    private int currentMergePos = 0;
    private int finishedMergeWorkerCount = 0;

    public AkkaAgent(long start) {
        this.start = start;
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        getSelf().tell(new StartGenerateMessage(), null);
    }

    public void onReceive(Object message) throws Exception {
        if (message instanceof StartGenerateMessage) {
            for (int i = 0; i < threadCount; i++) {
                getContext().actorOf(Props.create(WriteWorker.class, i), "actor-write-" + i);
            }
        }
        else if (message instanceof FinishWriteMessage) {
            FinishWriteMessage m = (FinishWriteMessage) message;
            int index = m.getIndex();
            long length = m.getTotalLength();
            getContext().actorOf(Props.create(MergeWorker.class, index, currentMergePos), "actor-merge-" + index);
            currentMergePos += length;
        }
        else if (message instanceof FinishMergeMessage) {
            if (++finishedMergeWorkerCount == threadCount) {
                // 如果已经合并结束了，那么就关闭整个akka system，释放资源
                getContext().system().shutdown();
                long end = System.currentTimeMillis();
                System.out.println("akka:" + (end - start));
            }
        }
    }
}
