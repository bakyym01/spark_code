package MultiThread;

import java.util.concurrent.atomic.AtomicReference;

public class SpinLockDemo {
    AtomicReference<Thread> spinLock = new AtomicReference<Thread>();
    public void myLock(){
        Thread thread = Thread.currentThread();
        while(!spinLock.compareAndSet(null,thread)){

        }
        System.out.println("lock thread"+"\t"+thread.getName());
    }

    public void myUnlock(){
        Thread thread = Thread.currentThread();
        spinLock.compareAndSet(thread,null);
        System.out.println("unleash thread"+"\t"+thread.getName());
    }

    public static void main(String[] args) {

        final SpinLockDemo spinLockDemo = new SpinLockDemo();
         new Thread("aa"){
            @Override
            public void run() {
                spinLockDemo.myLock();
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                spinLockDemo.myUnlock();
            }
        }.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        new Thread("bb"){
            @Override
            public void run() {
                spinLockDemo.myLock();
                spinLockDemo.myUnlock();
            }
        }.start();
    }
}
