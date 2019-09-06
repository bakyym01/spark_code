package DataStructure;

public class QuickSort {
    public static void main(String[] args) {
        int [] test={2,335,556,21,456,768,98,342,34,56,87};
        quickSort(test,0,test.length-1);
        for (int i = 0; i <test.length ; i++) {
            System.out.println(test[i]);
        }
    }
    public static void quickSort(int [] a,int low, int high){
        if(low>high){
            return;
        }
        int i=low;
        int j=high;
        int key=a[low];
        int temp=0;
        while(i<j){
            while(i<j&&a[j]>=key){
                j--;
            }
            while(i<j&&a[i]<=key){
                i++;
            }
            if(i<j){
                temp = a[i];
                a[i] = a[j];
                a[j] = temp;
            }
        }
        temp = a[i];
        a[i]=a[low];
        a[low]=temp;
        quickSort(a,low,i-1);
        quickSort(a,i+1,high);
    }
}
