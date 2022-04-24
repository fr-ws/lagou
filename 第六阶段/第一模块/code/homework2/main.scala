package lagou.cn.homework2

object main {

  def main(args: Array[String]): Unit = {
    println("- - - - - - - - - - 欢迎进入游戏世界- - - - - - - - - -\n" +
      "************************************\n" +
      "***************猜拳，开始***********\n" +
      "************************************")

    val user = new User
    val computer = new Computer

    println("请选择对战角色：（1.刘备  2.关羽  3.张飞）")
    val cID = readInt()
    computer.setName(cID)
    val cName = computer.name
    println("你选择了与" + cName + "对战")
    println("要开始了么？y/n")
    var startFlag = readLine()

    //    while循环游戏
    while (true) {
      val game = new Game(user, computer)
      if (startFlag == "y") {
        game.fightTimes += 1
        println("请出拳！1.剪刀 2.石头 3.步")
        val userFist = readInt()
        if (userFist == 1 || userFist == 2 || userFist == 3) {
          user.showFist(userFist)
        }
        else {
          print("输入不符合规范")
          user.showFist(3)
        }
        println(cName + "出拳！")
        computer.randomFist()
        //        判断胜负
        game.judge()
        print("是否开始下一轮?y/n")
        startFlag = readLine()
      } else {
        println("退出游戏！\n" +
          "- - - - - - - - - - - - - - - - - --  -- - - - - - -- - -  - - - - -\n"
          + cName + "VS" + user.name)
        println("姓名\t得分\t胜局\t和局\t负局")
        println(s"${user.name}\t${user.score}\t${user.win}\t${user.draw}\t${user.fail}\n${cName}\t${computer.score}\t${computer.win}\t${computer.draw}\t${computer.fail}")
        return
      }
    }

  }

}
