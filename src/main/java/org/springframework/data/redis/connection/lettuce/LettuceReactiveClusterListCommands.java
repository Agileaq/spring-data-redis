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

import java.nio.ByteBuffer;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.ReactiveClusterListCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rx.Observable;

/**
 * @author Christoph Strobl
 * @since 2016/10
 */
public class LettuceReactiveClusterListCommands extends LettuceReactiveListCommands implements ReactiveClusterListCommands {

	/**
	 * Create new {@link LettuceReactiveListCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveClusterListCommands(LettuceReactiveRedisConnection connection) {
		super(connection);
	}

	@Override
	public Flux<ReactiveRedisConnection.ByteBufferResponse<RPopLPushCommand>> rPopLPush(
			Publisher<RPopLPushCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).flatMap(command -> {

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getKey(), command.getDestination())) {
				return super.rPopLPush(Mono.just(command));
			}

			Observable<ByteBuffer> result = cmd.rpop(command.getKey().array())
					.concatMap(value -> cmd.lpush(command.getDestination().array(), value).map(x -> value)).map(ByteBuffer::wrap);

			return LettuceReactiveRedisConnection.<ByteBuffer> monoConverter().convert(result)
					.map(value -> new ReactiveRedisConnection.ByteBufferResponse<>(command, value));
		}));
	}

}
