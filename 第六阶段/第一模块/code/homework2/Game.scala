package lagou.cn.homework2
import scala.beans.BeanProperty
class Game(user:User,computer: Computer) {
  @BeanProperty var fightTimes:Int = 0

//  猜拳结果判断，并计算得分和胜负局数
  def judge(): Unit ={
    fightTimes += 1
    if(user.fist == computer.fist){
      user.draw += 1
      computer.draw +=1
      user.score += 1
      computer.score += 1
      println("结果！和局！下次继续努力")
    }
    else if(user.fist==1 & computer.fist == 3 || user.fist==2 & computer.fist == 1 || user.fist == 3 & (computer.fist == 2 ||  computer.fist == 1))
      {
        user.win += 1
        computer.fail += 1
        user.score += 2
        println("结果：恭喜，你赢啦！")
      }
    else {
      user.fail += 1
      computer.win += 1
      computer.score += 2
      println("结果：你输了，垃圾")
    }
  }


}
