package mytest.test1

object testscala {
  def main(args: Array[String]) {
    println(div(10, 5))
    println(div(10, 0))
    val x1 = div(10, 5).getOrElse(0)
    val x2 = div(10, 0).getOrElse(0)
    println(x1)
    println(x2)
  }
  def div(a: Int, b: Int): Option[Int] = {
    if (b != 0) Some(a / b) else None
  }

}