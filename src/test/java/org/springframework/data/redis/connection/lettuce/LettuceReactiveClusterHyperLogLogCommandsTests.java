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

import static org.hamcrest.core.Is.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.springframework.data.redis.connection.RedisClusterNode.*;
import static org.springframework.data.redis.connection.lettuce.LettuceReactiveCommandsTestsBase.*;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.test.util.LettuceRedisClusterClientProvider;

import com.lambdaworks.redis.cluster.api.sync.RedisClusterCommands;

/**
 * @author Christoph Strobl
 */
public class LettuceReactiveClusterHyperLogLogCommandsTests {

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
	public void pfCountWithMultipleKeysShouldReturnCorrectlyWhenKeysMapToSameSlot() {

		nativeCommands.pfadd(SAME_SLOT_KEY_1, new String[] { VALUE_1, VALUE_2 });
		nativeCommands.pfadd(SAME_SLOT_KEY_2, new String[] { VALUE_2, VALUE_3 });

		assertThat(connection.hyperLogLogCommands().pfCount(Arrays.asList(SAME_SLOT_KEY_1_BBUFFER, SAME_SLOT_KEY_2_BBUFFER))
				.block(), is(3L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void pfMergeShouldWorkCorrectlyWhenKeysMapToSameSlot() {

		nativeCommands.pfadd(SAME_SLOT_KEY_1, new String[] { VALUE_1, VALUE_2 });
		nativeCommands.pfadd(SAME_SLOT_KEY_2, new String[] { VALUE_2, VALUE_3 });

		assertThat(
				connection.hyperLogLogCommands()
						.pfMerge(SAME_SLOT_KEY_3_BBUFFER, Arrays.asList(SAME_SLOT_KEY_1_BBUFFER, SAME_SLOT_KEY_2_BBUFFER)).block(),
				is(true));

		assertThat(nativeCommands.pfcount(new String[] { SAME_SLOT_KEY_3 }), is(3L));
	}

}