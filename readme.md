# rxjs-kafkajs

> [rxjs](https://www.npmjs.com/package/rxjs) + [kafkajs](https://www.npmjs.com/package/kafkajs)

[![npm](https://shields.io/npm/v/rxjs-kafkajs)](https://www.npmjs.com/package/rxjs-kafkajs) [![Coverage Status](https://coveralls.io/repos/github/rxjs-kafkajs/badge.svg?branch=master)](https://coveralls.io/github/rxjs-kafkajs?branch=master)

## Usage

```typescript
import { RxKafka } from 'rxjs-kafkajs';

const rxKafka = new RxKafka({
	brokers: [
		`localhost:9092`,
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
						topic: 'topic-2',
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
```
