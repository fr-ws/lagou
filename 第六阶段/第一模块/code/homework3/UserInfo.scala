package lagou.cn.homework3

case class UserInfo(userName: String, location: String, startTime: Int, duration: Int) {}

object userCombine {
  //  /*
  def main(args: Array[String]): Unit = {
    val userInfoList: List[UserInfo] = List(
      UserInfo("UserA", "LocationA", 8, 60),
      UserInfo("UserA", "LocationA", 9, 60),
      UserInfo("UserB", "LocationB", 10, 60),
      UserInfo("UserB", "LocationB", 11, 80)
    )
    //按照userName 和 location 组合分组
    val userMap = userInfoList.groupBy(x => x.userName + "," + x.location)
    //   利用mapValues将集合中的值按照startTime升序排列
    val sortedUserMap = userMap.mapValues(_.sortBy(_.startTime))
    //    1、对同一个用户，在同一个位置，连续的多条记录进行合并 2、合并原则：开始时间取最早时间，停留 时长累计求和
    var firstTime = 0
    val combineUserMap = sortedUserMap.mapValues({ x =>
      firstTime = x.head.startTime
      var sum = x.map(_.duration).sum
      sum
    }
    )
    println(combineUserMap)
    combineUserMap.foreach {
      case (k, v) => println(s"$k,$firstTime,$v")
    }
  }
}
