package MultiThread;

import java.util.concurrent.CountDownLatch;

public class CountDownLatchDemo {
    public static void main(String[] args) throws InterruptedException {
        CountDownLatch countDownLatch = new java.util.concurrent.CountDownLatch(6);

        for (int i = 1; i <= 6; i++) {
            new Thread(()->{
                System.out.println(Thread.currentThread().getName()+"\t被灭掉");
                countDownLatch.countDown();
            }, CountryEnum.foreach_Country(i)).start();
        }

        countDownLatch.await();
        System.out.println("秦一统中国");
    }
}
