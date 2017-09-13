package mapreduce.metamode;

/**
 * 并行作业链
 * 例子: 给定前用户已分箱好的用户，并行执行作业计算出每个箱用户的平均声望值
 *  （用户ID,帖子数，声望值) 
 *  
 *  Mapper输出  GROUP_ALL_KEY, 声望值
 *  Reducer输出  GROUP_ALL_KEY, 总声望值/count
 *  
 *  对于一个大的数据集，这样的操作代价非常昂贵，因为一个Reducer将负责通过网络拉去所有中间结果键/对。Map是并行的。
 */
public class ParallelJobs {

}
