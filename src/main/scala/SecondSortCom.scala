class SecondSortCom(val key:String,val num:Int) extends Ordered[SecondSortCom]with Serializable {
  override def compare(that: SecondSortCom): Int = {
    if(this.key.compareTo(that.key)!=0){
      this.key.compareTo(that.key)
    }else{
      this.num-that.num
    }
  }
}
