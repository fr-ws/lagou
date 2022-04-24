package lagou.cn.homework1

class Beer {

  def drink(bottle: Int, cap: Int): Int = {
    val num = bottle / 3 + cap / 5

    if (num <= 0)
      return 0

    val modBot = bottle % 3
    val modCap = cap % 5

    drink(num + modBot, num + modCap) + num

  }

}

object Beer {
  def main(args: Array[String]): Unit = {
    val beer = new Beer
    val num = beer.drink(50, 50)
    println(s"喝了${num + 50}瓶，美滋滋")
  }
}