## snowflake
snowflake id generator，参考[https://github.com/twitter-archive/snowflake](https://github.com/twitter-archive/snowflake)

改动部分
1. 自动获取worker id
2. 优化尾数的部分，原来的在使用频率不高的情况下大量的0结尾的id
3. 原来数据中心做成分库用

## 依赖
1. spring框架，方便集成(非必须)
2. redis&redisson，ID自动获取(非必须)

## 例子
1. 直接使用
```
long id = SnowFlakeIdGenerator.nextNumber();
```
2. 关联id，自省到同一分片，可以基于id自发现分库分表索引，默认64
```
long shardId = 666L;
long id = SnowFlakeIdGenerator.nextNumber(shardId);
//eq is true
SnowFlakeIdGenerator.findRelateNumberLast(id) == (shardId % 64 == 0 ? 64L : shardId % 64);
```