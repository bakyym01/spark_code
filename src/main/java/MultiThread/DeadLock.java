package MultiThread;

public class DeadLock {
    public static void main(String[] args) {
        String LockA="LockA";
        String LockB="LockB";
        new Thread(new LockDemo(LockA,LockB),String.valueOf("AA")).start();
        new Thread(new LockDemo(LockB,LockA),String.valueOf("BB")).start();

    }

}

class LockDemo implements Runnable{
    private String lockA;
    private String lockB;
    public LockDemo(String lockA,String lockB){
        this.lockA=lockA;
        this.lockB=lockB;
    }
    @Override
    public void run() {
        synchronized (lockA){
            System.out.println(Thread.currentThread().getName()+"\t获取锁"+lockA);
            synchronized (lockB){
                System.out.println(Thread.currentThread().getName()+"\t获取锁"+lockB);
            }
        }
    }
}
