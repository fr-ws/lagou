package lagou.cn.homework2
import scala.beans.BeanProperty
import scala.util.Random

class Computer {
  @BeanProperty var name:String = _
  @BeanProperty var score:Int = 0
  @BeanProperty var fist:Int = 3
  @BeanProperty var win:Int = 0
  @BeanProperty var draw:Int = 0
  @BeanProperty var fail:Int = 0
  val cArray  = Array("刘备","关羽","张飞")

  def showFist(fist:Int): Unit ={
    fist match {
      case 1 => this.fist = 1; println(this.name + "出拳：剪刀")
      case 2 => this.fist = 2; println(this.name + "出拳：石头")
      case 3 => this.fist = 3; println(this.name + "出拳：步")
    }
  }

  def setName(cID:Int): Unit ={
    this.name =cArray(cID-1)
  }

//用随机数访问数组索引随机出拳
  def randomFist(): Unit ={
    val random = new Random()
    val fist = random.nextInt(4-1) + 1
    this.showFist(fist)
  }


}
