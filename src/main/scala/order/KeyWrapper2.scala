package order

/**
  * 封装要排序的Keys
  * @param firstKey
  * @param secondKey
  */
class KeyWrapper2(val firstKey: Int, val secondKey: Int) extends Ordered[KeyWrapper2] with Serializable {
  override def compare(that: KeyWrapper2): Int = {
    if (this.firstKey != that.firstKey) {
      return that.firstKey - this.firstKey
    } else {
      return that.secondKey - this.secondKey
    }
  }
}
