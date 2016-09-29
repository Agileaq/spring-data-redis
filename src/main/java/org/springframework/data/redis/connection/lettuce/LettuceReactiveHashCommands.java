/*
 * Copyright 2016 the original author or authors.
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
import java.util.Collections;
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveHashCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import reactor.core.publisher.Flux;
import rx.Observable;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public class LettuceReactiveHashCommands implements ReactiveHashCommands {

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveHashCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveHashCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null!");
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveHashCommands#hSet(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<HSetCommand>> hSet(Publisher<HSetCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {

				Observable<Boolean> result = ObjectUtils.nullSafeEquals(command.isUpsert(), Boolean.TRUE)
						? cmd.hset(command.getKey().array(), command.getField().array(), command.getValue().array())
						: cmd.hsetnx(command.getKey().array(), command.getField().array(), command.getValue().array());

				return LettuceReactiveRedisConnection.<Boolean> monoConverter().convert(result)
						.map(value -> new BooleanResponse<>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveHashCommands#hMGet(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<HGetCommand, ByteBuffer>> hMGet(Publisher<HGetCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {

				Observable<List<ByteBuffer>> result = null;

				if (command.getFields().size() == 1) {
					result = cmd.hget(command.getKey().array(), command.getFields().iterator().next().array())
							.map(ByteBuffer::wrap).map(val -> val != null ? Collections.singletonList(val) : Collections.emptyList());
				} else {
					result = cmd
							.hmget(command.getKey().array(),
									command.getFields().stream().map(ByteBuffer::array).toArray(size -> new byte[size][]))
							.map(val -> val != null ? ByteBuffer.wrap(val) : null).toList();
				}

				return LettuceReactiveRedisConnection.<List<ByteBuffer>> monoConverter().convert(result)
						.map(value -> new MultiValueResponse<>(command, value));
			});
		});
	}

}