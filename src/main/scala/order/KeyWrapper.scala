package order

/**
  * 封装要排序的Keys
  * @param firstKey
  * @param secondKey
  */
class KeyWrapper(val firstKey: Int, val secondKey: Int) extends Ordered[KeyWrapper] with Serializable {
  override def compare(that: KeyWrapper): Int = {
    if (this.firstKey != that.firstKey) {
      return this.firstKey - that.firstKey
    } else {
      return this.secondKey - that.secondKey
    }
  }
}
