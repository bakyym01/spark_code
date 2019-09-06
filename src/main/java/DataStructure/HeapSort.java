package DataStructure;

public class HeapSort {
    public static void main(String[] args) {
        int [] arr={12,13,89,56,78,87,34,323};
        int length=arr.length;
        buidHeap(arr);
        for (int i = 0; i <length ; i++) {
            swap(arr,0,length-1-i);
            adjustHeap(arr,0,length-i-1);
        }
        for (int i:arr
             ) {
            System.out.println(i);
        }
    }

    public static void buidHeap(int [] arr){
        int heapLength=arr.length;
        int parent=parent(heapLength-1);
        for (int i = parent; i >=0 ; i--) {
            adjustHeap(arr,i,heapLength);
        }

    }

    private static void adjustHeap(int[] arr, int i, int heapLength) {
        int left=left(i);
        int right=right(i);
        int smallest=-1;

        if(left<heapLength&&arr[left]<arr[i]){
            smallest=left;
        }else{
            smallest=i;
        }

        if(right<heapLength&&arr[smallest]>arr[right]){
            smallest=right;
        }

        if(i!=smallest){
            swap(arr,smallest,i);
            adjustHeap(arr,smallest,heapLength);
        }

    }

    public static int left(int i){
        return (i+1)*2-1;
    }
    public static int right(int i){
        return (i+1)*2;
    }
    public static int parent(int i){
        if(i==0)
            return  -1;
        return (i-1)/2;
    }
    public static void swap(int [] arr,int from,int to){
        int temp=arr[from];
        arr[from]=arr[to];
        arr[to]=temp;
    }
}
