package MultiThread;

import java.util.concurrent.Semaphore;

public class SemaphoreDemo {
    public static void main(String[] args) {
        Semaphore semo = new Semaphore(3,true);
        for (int i = 1; i < 7; i++) {
            new Thread(()->{
                try {
                    semo.acquire();
                    System.out.println(Thread.currentThread().getName()+"\t号车获得车位");
                    Thread.sleep(3000);
                    System.out.println(Thread.currentThread().getName()+"\t号车停车3秒后离开车位");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }finally {
                    semo.release();
                }
            }, String.valueOf(i)).start();
        }
    }
}
