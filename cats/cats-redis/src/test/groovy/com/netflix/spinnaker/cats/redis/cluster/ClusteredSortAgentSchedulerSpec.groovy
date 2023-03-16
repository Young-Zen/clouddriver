/*
 * Copyright 2023 THL A29 Limited, a Tencent company.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.cats.redis.cluster

import com.netflix.spinnaker.cats.agent.AgentExecution
import com.netflix.spinnaker.cats.agent.CachingAgent
import com.netflix.spinnaker.cats.agent.DefaultCacheResult
import com.netflix.spinnaker.cats.agent.ExecutionInstrumentation
import com.netflix.spinnaker.cats.cache.CacheData
import com.netflix.spinnaker.cats.cluster.DefaultAgentIntervalProvider
import com.netflix.spinnaker.cats.cluster.NodeStatusProvider
import com.netflix.spinnaker.cats.test.TestAgent
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import spock.lang.Specification
import spock.lang.Subject

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class ClusteredSortAgentSchedulerSpec extends Specification {

  @Subject
  ClusteredSortAgentScheduler clusteredSortAgentScheduler

  Jedis jedis
  JedisPool jedisPool
  int parallelism = 2
  AgentExecution agentExecution = Mock(CachingAgent.CacheExecution)
  ExecutionInstrumentation executionInstrumentation = Mock(ExecutionInstrumentation)

  def setup() {
    jedis = Mock(Jedis)
    jedisPool = Stub(JedisPool) {
      getResource() >> jedis
    }
    jedis.scriptLoad(_) >> "testScriptSha"
    jedis.time() >> ["1678784468", "374338"]
    def interval = new DefaultAgentIntervalProvider(6000000)
    clusteredSortAgentScheduler = new ClusteredSortAgentScheduler(jedisPool, { { -> false } } as NodeStatusProvider, interval, parallelism)
  }

  def "test runningAgents Semaphore"() {
    given:
    jedis.zrangeByScore(ClusteredSortAgentScheduler.WORKING_SET, _, _) >> []
    jedis.zrangeByScore(ClusteredSortAgentScheduler.WAITING_SET, _, _) >> ["testAgentType"]

    when:
    clusteredSortAgentScheduler.saturatePool()

    then:
    clusteredSortAgentScheduler.runningAgents.map({it -> it.availablePermits()}).orElse(-1) == parallelism
  }

  def "test runningAgents Semaphore with exception"() {
    given:
    def agent1 = new TestAgent()
    def agent2 = new TestAgent()
    def latch = new CountDownLatch(1)

    and:
    jedis.zrangeByScore(ClusteredSortAgentScheduler.WORKING_SET, _, _) >> []
    jedis.zrangeByScore(ClusteredSortAgentScheduler.WAITING_SET, _, _) >> [agent1.getAgentType(), agent2.getAgentType()]
    jedis.scriptExists(_) >> true
    clusteredSortAgentScheduler.schedule(agent1, agentExecution, executionInstrumentation)
    clusteredSortAgentScheduler.schedule(agent2, agentExecution, executionInstrumentation)
    jedis.evalsha(_ as String, _ as List<String>, _ as List<String>) >> "testReleaseScore" >> { throw new RuntimeException("fail") } >> "testReleaseScore"
    agentExecution.executeAgentWithoutStore(_) >> new DefaultCacheResult(new HashMap<String, Collection<CacheData>>()) {}
    agentExecution.storeAgentResult(_, _) >> { latch.countDown() }

    when:
    clusteredSortAgentScheduler.saturatePool()
    latch.await(10, TimeUnit.SECONDS)

    then:
    clusteredSortAgentScheduler.runningAgents.map({it -> it.availablePermits()}).orElse(-1) == parallelism
  }
}
