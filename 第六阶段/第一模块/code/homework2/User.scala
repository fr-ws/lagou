package lagou.cn.homework2

import scala.beans.BeanProperty

class User() {
  @BeanProperty var name:String = "游客"
  @BeanProperty var score:Int = 0
  @BeanProperty var fist:Int = 3
  @BeanProperty var win:Int = 0
  @BeanProperty var draw:Int = 0
  @BeanProperty var fail:Int = 0

  def showFist(fist:Int): Unit = {
    fist match {
      case 1 => this.fist = 1; println("你出拳：剪刀")
      case 2 => this.fist = 2; println("你出拳：石头")
      case 3 => this.fist = 3; println("你出拳：步")
    }
  }





}
