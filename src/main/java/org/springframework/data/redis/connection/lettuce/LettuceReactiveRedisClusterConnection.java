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

import org.springframework.data.redis.connection.ReactiveRedisClusterConnection;

import com.lambdaworks.redis.cluster.RedisClusterClient;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public class LettuceReactiveRedisClusterConnection extends LettuceReactiveRedisConnection
		implements ReactiveRedisClusterConnection {

	public LettuceReactiveRedisClusterConnection(RedisClusterClient client) {
		super(client);
	}

	@Override
	public LettuceReactiveClusterKeyCommands keyCommands() {
		return new LettuceReactiveClusterKeyCommands(this);
	}

	@Override
	public LettuceReactiveClusterListCommands listCommands() {
		return new LettuceReactiveClusterListCommands(this);
	}

	@Override
	public LettuceReactiveClusterSetCommands setCommands() {
		return new LettuceReactiveClusterSetCommands(this);
	}

	@Override
	public LettuceReactiveClusterZSetCommands zSetCommands() {
		return new LettuceReactiveClusterZSetCommands(this);
	}

	@Override
	public LettuceReactiveClusterHyperLogLogCommands hyperLogLogCommands() {
		return new LettuceReactiveClusterHyperLogLogCommands(this);
	}

	@Override
	public LettuceReactiveClusterStringCommands stringCommands() {
		return new LettuceReactiveClusterStringCommands(this);
	}

	@Override
	public LettuceReactiveClusterGeoCommands geoCommands() {
		return new LettuceReactiveClusterGeoCommands(this);
	}

	@Override
	public LettuceReactiveClusterHashCommands hashCommands() {
		return new LettuceReactiveClusterHashCommands(this);
	}

	@Override
	public LettuceReactiveClusterNumberCommands numberCommands() {
		return new LettuceReactiveClusterNumberCommands(this);
	}
}
