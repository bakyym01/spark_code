package MultiThread;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class CyclicBarrierDemo {
    public static void main(String[] args) {
        CyclicBarrier barrier = new CyclicBarrier(7,()->{
            System.out.println("召唤神龙");
        });

        for(int i=1;i<=7;i++){
            final int tmp = i;
            new Thread(()->{
                System.out.println(Thread.currentThread().getName()+"\t收集到第"+tmp+"个龙珠");
                try {
                    barrier.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
            },String.valueOf(i)).start();
        }

    }


}
