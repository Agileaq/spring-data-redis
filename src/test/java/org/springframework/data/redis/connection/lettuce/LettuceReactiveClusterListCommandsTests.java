/*
 * Copyright 2016. the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2016. the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.redis.connection.lettuce;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;
import static org.springframework.data.redis.connection.lettuce.LettuceReactiveCommandsTestsBase.*;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;

import com.lambdaworks.redis.cluster.api.sync.RedisClusterCommands;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.data.redis.connection.ReactiveListCommands;
import org.springframework.data.redis.test.util.LettuceRedisClientProvider;
import org.springframework.data.redis.test.util.LettuceRedisClusterClientProvider;

/**
 * @author Christoph Strobl
 */
public class LettuceReactiveClusterListCommandsTests {

	public static @ClassRule LettuceRedisClusterClientProvider clientProvider = LettuceRedisClusterClientProvider.local();

	RedisClusterCommands<String, String> nativeCommands;
	LettuceReactiveRedisClusterConnection connection;

	@Before
	public void before() {
		assumeThat(clientProvider.test(), is(true));
		nativeCommands = clientProvider.getClient().connect().sync();
		connection = new LettuceReactiveRedisClusterConnection(clientProvider.getClient());
	}

	@After
	public void tearDown() {

		nativeCommands.flushall();
		nativeCommands.close();

		connection.close();
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void bRPopLPushShouldWorkCorrectlyWhenAllKeysMapToSameSlot() {

		nativeCommands.rpush(SAME_SLOT_KEY_1, VALUE_1, VALUE_2, VALUE_3);
		nativeCommands.rpush(SAME_SLOT_KEY_2, VALUE_1);

		ByteBuffer result = connection.listCommands().bRPopLPush(SAME_SLOT_KEY_1_BBUFFER, SAME_SLOT_KEY_2_BBUFFER, Duration.ofSeconds(1))
				.block();

		assertThat(result, is(equalTo(VALUE_3_BBUFFER)));
		assertThat(nativeCommands.llen(SAME_SLOT_KEY_2), is(2L));
		assertThat(nativeCommands.lindex(SAME_SLOT_KEY_2, 0), is(equalTo(VALUE_3)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void blPopShouldReturnFirstAvailableWhenAllKeysMapToTheSameSlot() {

		nativeCommands.rpush(SAME_SLOT_KEY_1, VALUE_1, VALUE_2, VALUE_3);

		ReactiveListCommands.PopResult result = connection.listCommands()
				.blPop(Arrays.asList(SAME_SLOT_KEY_1_BBUFFER, SAME_SLOT_KEY_2_BBUFFER), Duration.ofSeconds(1L)).block();
		assertThat(result.getKey(), is(equalTo(SAME_SLOT_KEY_1_BBUFFER)));
		assertThat(result.getValue(), is(equalTo(VALUE_1_BBUFFER)));
	}



}
