import process from 'node:process';

import * as Rx from 'rxjs';
import getPort from 'get-port';
import waitPort from 'wait-port';
import type { Admin } from 'kafkajs';
import { logLevel, Kafka } from 'kafkajs';
import promiseRetry from 'p-retry';
import { DockerCompose } from '@futpib/docker-compose';

import type { ExecutionContext, TestInterface } from 'ava'; // eslint-disable-line ava/use-test
import anyTest from 'ava';

import { RxKafka } from '.';

const {
	TEST_ENABLE_KAFKA_UI,
} = process.env;

Error.stackTraceLimit = 100;

interface TestContext {
	kafka: Kafka;
	kafkaPort: number;
	kafkaAdmin: Admin;
	dockerCompose: DockerCompose;
}

const test = anyTest as TestInterface<TestContext>;

test.before(async t => {
	const kafkaPort = await getPort();

	const dockerCompose = new DockerCompose({
		projectName: 'rxjs-kafkajs-test',
		file: {
			version: '3.9',
			services: {
				zookeeper: {
					image: 'confluentinc/cp-zookeeper:7.0.0',
					environment: {
						ZOOKEEPER_CLIENT_PORT: '2181',
					},
				},

				kafka: {
					image: 'confluentinc/cp-kafka:7.0.0',
					depends_on: [
						'zookeeper',
					],
					environment: {
						KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181',
						KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT',
						KAFKA_ADVERTISED_LISTENERS: `PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:${kafkaPort}`,
						KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: '1',
						KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
						KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
						KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: '0',
					},
					ports: [
						`${kafkaPort}:${kafkaPort}`,
					],
				},

				'kafka-ui': {
					image: 'provectuslabs/kafka-ui:0.2.1',
					depends_on: [
						'kafka',
						'zookeeper',
					],
					ports: [
						'8080:8080',
					],
					environment: {
						KAFKA_CLUSTERS_0_NAME: 'local',
						KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: 'kafka:9092',
						KAFKA_CLUSTERS_0_ZOOKEEPER: 'zookeeper:2181',
					},
				},
			},
		},
	});

	Object.assign(t.context, {
		kafkaPort,
		dockerCompose,
	});

	await dockerCompose.up({
		scale: {
			'kafka-ui': TEST_ENABLE_KAFKA_UI === 'true' ? 1 : 0,
		},
	});

	await waitPort({
		port: kafkaPort,
		timeout: 60_000,
		output: 'silent',
	});

	const kafka = new Kafka({
		brokers: [
			`localhost:${kafkaPort}`,
		],
	});

	const kafkaAdmin = kafka.admin();

	Object.assign(t.context, {
		kafka,
		kafkaAdmin,
	});

	await promiseRetry(async () => {
		await kafkaAdmin.connect();
	});
});

test.after.always(async (t: ExecutionContext<Partial<TestContext>>) => {
	const {
		kafkaAdmin,
		dockerCompose,
	} = t.context;

	await kafkaAdmin?.disconnect();

	await dockerCompose?.rm({
		stop: true,
		force: true,
	});
});

test.serial('cosumeMergeMapProduce', async t => {
	t.timeout(120_000);

	const {
		kafkaPort,
		kafkaAdmin,
	} = t.context;

	await kafkaAdmin.createTopics({
		waitForLeaders: true,
		topics: [
			{
				topic: 'topic-1',
			},
		],
	});

	const rxKafka = new RxKafka({
		brokers: [
			`localhost:${kafkaPort}`,
		],
		logLevel: logLevel.DEBUG,
	});

	const producer = rxKafka.producer({
		maxInFlightRequests: 1,
		idempotent: true,
	});

	await producer.connect();

	await producer.send({
		topic: 'topic-1',
		messages: [
			{
				value: 'bar',
			},
			{
				value: 'foo',
			},
		],
	});

	const observable = rxKafka.consumeMergeMapProduce({
		groupId: 'group',
		transactionalId: 'transactional-id-1',
		topic: 'topic-1',
		async * project({ message }) {
			if (message.value?.toString() === 'foo') {
				yield {
					topicMessages: [
						{
							topic: 'topic-1',
							messages: [
								{
									value: 'foo1',
								},
								{
									value: 'foo2',
								},
							],
						},
					],
				};
			}
		},
	});

	await Rx.firstValueFrom(observable);

	await producer.disconnect();

	const actualTopicOffsets = await kafkaAdmin.fetchTopicOffsets('topic-1');

	t.deepEqual(actualTopicOffsets, [
		{
			high: '2',
			low: '0',
			offset: '2',
			partition: 0,
		},
	]);
});
