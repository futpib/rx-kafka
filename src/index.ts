import debug from 'debug';
import * as Rx from 'rxjs';
import * as Rxo from 'rxjs/operators';
import type { ConsumerConfig, EachMessagePayload, ProducerBatch, ProducerConfig, ConsumerSubscribeTopic } from 'kafkajs';
import { Kafka } from 'kafkajs';

const log = debug('rxjs-kafkajs');

export interface RxKafkaError extends Error {
	transactionAbortError?: Error;
}

export type ConsumeMergeMapProduce = {
	project: (value: EachMessagePayload) => AsyncGenerator<ProducerBatch>;

	transactionalId: string;
} & ConsumerConfig & ConsumerSubscribeTopic & ProducerConfig;

export class RxKafka extends Kafka {
	consumeMergeMapProduce({
		// Rx.mergeMap
		project,

		// ConsumerConfig
		groupId,
		partitionAssigners,
		sessionTimeout,
		rebalanceTimeout,
		heartbeatInterval,
		maxBytesPerPartition,
		minBytes,
		maxBytes,
		maxWaitTimeInMs,
		readUncommitted,
		rackId,

		// ConsumerSubscribeTopic
		topic,
		fromBeginning = true,

		// ProducerConfig
		transactionalId,
		createPartitioner,
		transactionTimeout,
		// https://kafka.js.org/docs/transactions#sending-messages-within-a-transaction
		idempotent = true,

		// ConsumerConfig & ProducerConfig
		// https://kafka.js.org/docs/transactions#sending-messages-within-a-transaction
		maxInFlightRequests = 1,
		allowAutoTopicCreation,
		retry,
		metadataMaxAge,
	}: ConsumeMergeMapProduce): Rx.Observable<void> {
		const consumerObservable = new Rx.Observable<EachMessagePayload>(subscriber => {
			const consumer = this.consumer({
				groupId,
				partitionAssigners,
				metadataMaxAge,
				sessionTimeout,
				rebalanceTimeout,
				heartbeatInterval,
				maxBytesPerPartition,
				minBytes,
				maxBytes,
				maxWaitTimeInMs,
				retry,
				allowAutoTopicCreation,
				maxInFlightRequests,
				readUncommitted,
				rackId,
			});

			const setupPromise = (async () => {
				try {
					log('consumer.connect', { groupId });
					await consumer.connect();

					await consumer.subscribe({
						topic,
						fromBeginning,
					});

					await consumer.run({
						autoCommit: false,
						async eachMessage(message) {
							log('consumer.eachMessage');
							subscriber.next(message);
						},
					});
				} catch (error: unknown) {
					subscriber.error(error);
				}
			})();

			return async () => {
				try {
					await setupPromise;
				} finally {
					log('consumer.disconnect', { groupId });
					await consumer.disconnect();
				}
			};
		});

		const producerGenerator = (async function * (this: RxKafka): AsyncGenerator<void, void, undefined | EachMessagePayload> {
			const producer = this.producer({
				createPartitioner,
				retry,
				metadataMaxAge,
				allowAutoTopicCreation,
				idempotent,
				transactionalId,
				transactionTimeout,
				maxInFlightRequests,
			});

			log('producer.connect');
			await producer.connect();

			try {
				let message = yield;

				/* eslint-disable no-await-in-loop */
				while (message) {
					log('producer.transaction');
					const transaction = await producer.transaction();

					try {
						for await (const producerBatch of project(message)) {
							log('transaction.sendBatch');
							await transaction.sendBatch(producerBatch);
						}

						log('transaction.sendOffsets');
						await transaction.sendOffsets({
							consumerGroupId: groupId,
							topics: [
								{
									topic: message.topic,
									partitions: [
										{
											partition: message.partition,
											offset: message.message.offset,
										},
									],
								},
							],
						});

						log('transaction.commit');
						await transaction.commit();
					} catch (error: unknown) {
						try {
							log('transaction.abort');
							await transaction.abort();
						} catch (transactionAbortError: unknown) {
							(error as RxKafkaError).transactionAbortError = (transactionAbortError as RxKafkaError);

							throw error;
						}

						throw error;
					}

					message = yield;
				}
				/* eslint-enable no-await-in-loop */
			} finally {
				log('producer.disconnect');
				await producer.disconnect();
			}
		}).call(this);

		return (
			consumerObservable
				.pipe(Rxo.mergeMap(async message => {
					await producerGenerator.next(message);
				}, 1))
				.pipe(Rxo.catchError(async error => {
					await producerGenerator.throw(error);
				}))
				.pipe(Rxo.finalize(async () => {
					await producerGenerator.next(undefined);
				}))
		);
	}
}
