package test;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateUtils;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 *  优化ID尾数&支持自动获取workerId,方便容器化部署 
 */
@Component
public class SnowFlakeIdGenerator implements InitializingBean {

    /**
     * 日志
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowFlakeIdGenerator.class);

    /**
     * 初始化worker id需要的分布式锁实现，也可以选ZK或其它分布式啥的
     */
    @Autowired(required = false)
    private RedissonClient redissonClient;

    /**
     * 初始时间(2021-02-01)
     */
    private static final long INITIAL_TIME_STAMP = 1612137600000L;

    /**
     * 序列号 1024，每秒1024*1000=100w
     */
    private static final long SEQUENCE_BITS = 10L;

    private static final long SEQUENCE_MASK = ~(-1L << SEQUENCE_BITS);

    /**
     * 机器ID，支持256
     */
    private static final long WORK_ID_BITS = 8L;

    /**
     * 最大机器ID索引
     */
    private static final long MAX_WORKER_ID = ~(-1L << WORK_ID_BITS);

    private static final long WORKERID_OFFSET = SEQUENCE_BITS;

    /**
     * 数据中心ID
     */
    private static final long DATACENTER_ID_BITS = 6L;

    /**
     * 最大数据中心索引
     */
    private static final long MAX_DATACENTER_ID = ~(-1L << DATACENTER_ID_BITS);

    private static final long DATACENTERID_OFFSET = WORK_ID_BITS + SEQUENCE_BITS;

    /**
     * 分库ID，最多支持64分库
     */
    private static final long RELATE_ID_SHARDING = 1L << DATACENTER_ID_BITS;

    private static final long TIMESTAMP_OFFSET = DATACENTER_ID_BITS + WORK_ID_BITS + SEQUENCE_BITS;

    private long workerId;

    private long datacenterId;

    private long sequence = 0L;

    private long lastTimestamp = -1L;

    private Random random = new Random();

    public SnowFlakeIdGenerator() {}

    private static SnowFlakeIdGenerator DEFAULT_GENERATOR = null;

    /**
     * 自动分布式获取id
     */
    private static final ThreadFactory NAMED_THREAD_FACTORY =
        new ThreadFactoryBuilder().setNameFormat("SnowFlakeIdGenerator-workerId-expired-%d").build();
    private static final ScheduledExecutorService WORKERD_EXPIRED_SCHEDULED =
        new ScheduledThreadPoolExecutor(1, NAMED_THREAD_FACTORY);

    private SnowFlakeIdGenerator(long workerId, RedissonClient redissonClient) {
        if (workerId > MAX_WORKER_ID || workerId < 0L) {
            throw new IllegalArgumentException(
                String.format("WorkerID can't be greater than %d or less than 0", MAX_WORKER_ID));
        }
        LOGGER.info("SnowFlakeIdGenerator init with workId:{}", workerId);
        this.workerId = workerId;
        this.redissonClient = redissonClient;
    }

    private long tillNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }

    private static long getDefaultWorkId() {
        Random rnd = new Random();
        try {
            NetworkInterface network = null;
            Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
            while (en.hasMoreElements()) {
                NetworkInterface nint = en.nextElement();
                if (!nint.isLoopback() && nint.getHardwareAddress() != null) {
                    network = nint;
                    break;
                }
            }
            if (network != null) {
                byte[] mac = network.getHardwareAddress();
                byte rndByte = (byte)(rnd.nextInt() & 0x000000FF);

                // take the last byte of the MAC address and a random byte as datacenter ID
                return (((0x000000FF & (long)mac[mac.length - 1]) | (0x0000FF00 & (((long)rndByte) << 8))) >> 6)
                    % MAX_WORKER_ID;
            }
        } catch (Exception e) {
            LOGGER.error("getDefaultWorkId", e);
        }
        return rnd.nextLong() % MAX_WORKER_ID;
    }

    /**
     * 生成ID
     * 
     * @return
     */
    public synchronized long nextId() {
        long timeStamp = System.currentTimeMillis();
        if (timeStamp < lastTimestamp) {
            throw new RuntimeException("The current time less than last time");
        }
        if (timeStamp == lastTimestamp) {
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (0L == sequence) {
                timeStamp = tillNextMillis(lastTimestamp);
            }
        } else {
            // 防止并发不够高从0开始的问题，导致取模不随机
            sequence = random.nextInt(64) & SEQUENCE_MASK;
        }
        lastTimestamp = timeStamp;

        return (timeStamp - INITIAL_TIME_STAMP) << TIMESTAMP_OFFSET | (datacenterId << DATACENTERID_OFFSET)
            | (workerId << WORKERID_OFFSET) | sequence;
    }

    // --start分库用
    /**
     * 生成带分片信息的ID
     * 
     * @param relateNumber
     * @return
     */
    public synchronized long nextId(long relateNumber) {
        long timeStamp = System.currentTimeMillis();
        if (timeStamp < lastTimestamp) {
            throw new RuntimeException("The current time less than last time");
        }
        if (timeStamp == lastTimestamp) {
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (0 == sequence) {
                timeStamp = tillNextMillis(lastTimestamp);
            }
        } else {
            // 防止并发不够高从0开始的问题，导致取模不随机
            sequence = random.nextInt(100) & SEQUENCE_MASK;
        }
        lastTimestamp = timeStamp;

        long tmpDatacenterId = relateNumber % RELATE_ID_SHARDING;
        return (timeStamp - INITIAL_TIME_STAMP) << TIMESTAMP_OFFSET | (tmpDatacenterId << DATACENTERID_OFFSET)
            | (workerId << WORKERID_OFFSET) | sequence;
    }

    public long findRelateIdLast(long number) {
        return (number >> DATACENTERID_OFFSET) & MAX_DATACENTER_ID;
    }
    // ---end分库用

    // --- start对外方法
    public static long nextNumber() {
        return DEFAULT_GENERATOR.nextId();
    }

    public static long nextNumber(long relateNumber) {
        return DEFAULT_GENERATOR.nextId(relateNumber);
    }

    public static long findRelateNumberLast(long number) {
        long last = DEFAULT_GENERATOR.findRelateIdLast(number);
        if (last == 0L) {
            return RELATE_ID_SHARDING;
        }
        return last;
    }

    /**
     * 外部shutdown用
     * 
     * @return
     */
    public static boolean destory() {
        try {
            WORKERD_EXPIRED_SCHEDULED.shutdown();
            return DEFAULT_GENERATOR.shutdown();
        } catch (Exception e) {
            LOGGER.warn("Stop ERROR", e);
        }
        return true;
    }
    // --- end 对外方法

    /**
     * 初始化，融入spring管理
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        // 有配置，直接走配置
        long workerId = NumberUtils.toLong(System.getProperty("WORK_ID"), -1L);
        final String hostIp = getHostIp();
        if (workerId < 0L && hostIp != null) {
            // REDIS
            workerId = fetchRedisWorkerId(hostIp);
        }
        if (workerId < 0L) {
            // random
            workerId = getDefaultWorkId();
        }
        SnowFlakeIdGenerator.DEFAULT_GENERATOR = new SnowFlakeIdGenerator(workerId, redissonClient);
        if (redissonClient != null) {
            final String expireKey = buildKey(workerId);
            // 设置占用
            setnx(redissonClient, expireKey, hostIp, DateUtils.MILLIS_PER_MINUTE);
            WORKERD_EXPIRED_SCHEDULED.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        boolean flag = expire(redissonClient, expireKey, DateUtils.MILLIS_PER_MINUTE);
                        LOGGER.info("WORKERD_EXPIRED_SCHEDULED host:{}, expire {} result:{}", hostIp, expireKey, flag);
                    } catch (Exception e) {
                        LOGGER.error("expire key {} {}", expireKey, e);
                    }
                }
            }, 0L, DateUtils.MILLIS_PER_SECOND * 20, TimeUnit.MILLISECONDS);
        }
    }

    private long fetchRedisWorkerId(String hostIp) {
        if (redissonClient == null) {
            return -1L;
        }
        // 循环简单处理
        for (long i = MAX_WORKER_ID; i >= 0L; i--) {
            if (setnx(redissonClient, buildKey(i), hostIp, DateUtils.MILLIS_PER_MINUTE)) {
                return i;
            }
        }
        throw new RuntimeException("All redis workerId used!!");
    }

    /**
     * 获取IPV4地址
     * 
     * @return
     */
    private String getHostIp() {
        try {
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = (NetworkInterface)allNetInterfaces.nextElement();
                Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress ip = (InetAddress)addresses.nextElement();
                    if (ip != null && ip instanceof Inet4Address && !ip.isLoopbackAddress()
                        && ip.getHostAddress().indexOf(":") == -1) {
                        return ip.getHostAddress();
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("getHostIp", e);
        }
        throw new RuntimeException("Not found host IPV4 address");
    }

    private boolean shutdown() {
        if (redissonClient != null) {
            String key = buildKey(workerId);
            boolean flag = del(redissonClient, key);
            LOGGER.info("WORKERD_EXPIRED_SCHEDULED host:{}, delete {} result:{}", getHostIp(), key, flag);
        }
        return false;
    }

    /**
     * for redisson wrapper
     */
    private String buildKey(long workerId) {
        return String.format("SOMEPREFIX:SnowFlakeIdGenerator:%d", workerId);
    }

    private boolean del(RedissonClient redissonClient, String key) {
        try {
            return redissonClient.getBucket(key).delete();
        } catch (Exception e) {
            LOGGER.error("RedissonWrapper del", e);
        }
        return false;
    }

    private boolean expire(RedissonClient redissonClient, String key, long timeMillis) {
        try {
            return redissonClient.getBucket(key).expire(timeMillis, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOGGER.error("RedissonWrapper expire", e);
        }
        return false;
    }

    private <T> boolean setnx(RedissonClient redissonClient, String key, T obj, long timeMillis) {
        try {
            RBucket<T> bucket = redissonClient.getBucket(key);
            return bucket.trySet(obj, timeMillis, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOGGER.error("RedissonWrapper setnx with time", e);
        }
        return false;
    }
}
