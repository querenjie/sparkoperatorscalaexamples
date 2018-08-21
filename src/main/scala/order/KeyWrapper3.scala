package order

/**
  * 封装要排序的Keys
  * @param firstKey
  * @param secondKey
  */
class KeyWrapper3(val firstKey: Int, val secondKey: Int) extends Ordered[KeyWrapper3] with Serializable {
  override def compare(that: KeyWrapper3): Int = {
    if (this.firstKey != that.firstKey) {
      return that.firstKey - this.firstKey
    } else {
      return this.secondKey - that.secondKey
    }
  }
}
