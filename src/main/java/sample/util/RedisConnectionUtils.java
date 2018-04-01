package sample.util;

import java.util.HashSet;
import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

public class RedisConnectionUtils {

    public static JedisCluster getJedisCluster() {
        
        JedisPoolConfig jedisConfig = new JedisPoolConfig();
        jedisConfig.setMaxTotal(60000);
        jedisConfig.setMaxIdle(100);
        jedisConfig.setMinIdle(8);
        jedisConfig.setMaxWaitMillis(10000);
        jedisConfig.setTestOnBorrow(true);
        jedisConfig.setTestOnReturn(true);

        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
        // Jedis Cluster will attempt to discover cluster nodes automatically
        jedisClusterNodes.add(new HostAndPort("192.168.186.136", 7000));
        jedisClusterNodes.add(new HostAndPort("192.168.186.136", 7001));
        jedisClusterNodes.add(new HostAndPort("192.168.186.136", 7002));
        jedisClusterNodes.add(new HostAndPort("192.168.186.136", 7003));
        jedisClusterNodes.add(new HostAndPort("192.168.186.136", 7004));
        jedisClusterNodes.add(new HostAndPort("192.168.186.136", 7005));
        JedisCluster jedisCluster = new JedisCluster(jedisClusterNodes, jedisConfig);

        return jedisCluster;
    }

    public static Jedis getJedis() {
        
        Jedis jedis = new Jedis("localhost");

        return jedis;
    }

}
