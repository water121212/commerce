case class SortedKey(clickCount:Long,orderCount:Long, payCount:Long) extends Ordered[SortedKey]{
  // compare(that: SortedKey)
  // this compare that
  // this < that :  this compare that < 0
  // this > that : this compare that > 0
  override def compare(that: SortedKey): Int = {
    if(this.clickCount - that.clickCount !=0){
      return (this.clickCount - that.clickCount).toInt
    }else if(this.orderCount - that.orderCount !=0){
      return (this.orderCount - that.orderCount).toInt
    }else if (this.payCount - that.payCount !=0){
      return (this.payCount - that.payCount).toInt
    }
    0
  }
}

