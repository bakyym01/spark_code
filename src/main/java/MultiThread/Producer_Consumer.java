package MultiThread;

public class Producer_Consumer {
    public static void main(String[] args) {
        Phone app = new Phone(0);
        new Thread(()->{
            for (int i = 0; ; i++) {
                app.produce();
            }

        },String.valueOf("producer")).start();
        new Thread(()->{
            for (int i = 0;; i++) {
                app.consume();
            }

        },String.valueOf("comsumer")).start();
    }

}

class Phone{
    private int number;
    public Phone(int number){
        this.number=number;
    }

    public synchronized void produce(){
        if(number>5){
            try {
                this.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        number++;
        System.out.println("手机生产了"+number);
        this.notifyAll();
    }

    public synchronized void consume(){
        if(number==0){
            try {
                this.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        number--;
        System.out.println("手机消费了"+number);
        this.notifyAll();
    }
}
