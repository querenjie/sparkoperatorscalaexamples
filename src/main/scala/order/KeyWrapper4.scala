package order

/**
  * 封装要排序的Keys
  * @param firstKey
  * @param secondKey
  */
class KeyWrapper4(val firstKey: Int, val secondKey: Int) extends Ordered[KeyWrapper4] with Serializable {
  override def compare(that: KeyWrapper4): Int = {
    if (this.firstKey != that.firstKey) {
      return this.firstKey - that.firstKey
    } else {
      return that.secondKey - this.secondKey
    }
  }
}
