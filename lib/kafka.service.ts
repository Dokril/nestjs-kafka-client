import { Injectable, Logger, LoggerService, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import {
	Consumer,
	Kafka,
	Producer,
	RecordMetadata,
	Admin,
	SeekEntry,
	TopicPartitionOffsetAndMetadata,
	Offsets,
	EachMessagePayload,
	logLevel,
	LogEntry,
} from 'kafkajs';
import { KafkaModuleOption, KafkaMessageSend, KafkaTransaction, IHeaders } from './interfaces';

import { SUBSCRIBER_MAP, SUBSCRIBER_OBJECT_MAP } from './kafka.decorator';
import { randomUUID } from 'crypto';
import { EventEmitter } from 'stream';

@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
	private kafka: Kafka;
	private admin: Admin;
	private producer: Producer;
	private consumer: Consumer;

	private autoConnect: boolean;
	private options: KafkaModuleOption;
	private logger: LoggerService;
	private responseTopic: string;
	private responseEvents: EventEmitter;

	constructor(options: KafkaModuleOption) {
		const { client, consumer: consumerConfig, producer: producerConfig, autoConnect } = options.clientConfig;
		this.options = options;
		this.logger = options.logger ? options.logger : new Logger(options.name);

		this.kafka = new Kafka({
			...client,
			logCreator: (level: logLevel) => {
				return (logInfo: LogEntry) => {
					switch (level) {
						case logLevel.DEBUG:
							this.logger.debug(logInfo.log);
							break;
						case logLevel.INFO:
							this.logger.log(logInfo.log);
							break;
						case logLevel.WARN:
							this.logger.warn(logInfo.log);
							break;
						case logLevel.ERROR:
							this.logger.error(logInfo.log);
							break;
						case logLevel.NOTHING:
						default:
							break;
					}
				};
			},
		});

		const { groupId } = consumerConfig;
		const consumerOptions = Object.assign({ groupId: `${groupId}-client` }, consumerConfig);
		this.autoConnect = autoConnect ?? true;
		this.consumer = this.kafka.consumer(consumerOptions);
		this.producer = this.kafka.producer(producerConfig);
		this.admin = this.kafka.admin();
	}

	async onModuleInit(): Promise<void> {
		await this.connect();
		this.initReply();
		SUBSCRIBER_MAP.forEach(async (functionRef, topic) => {
			await this.subscribe(topic);
		});
		this.bindAllTopicToConsumer();
	}

	private initReply() {
		if (this.options.replayMode) {
			this.responseTopic = this.options.name.toLowerCase() + '_response';
			this.responseEvents = new EventEmitter();
			SUBSCRIBER_MAP.set(this.responseTopic, null);
		}
	}

	async onModuleDestroy(): Promise<void> {
		await this.disconnect();
	}

	async connect(): Promise<void> {
		if (!this.autoConnect) {
			return;
		}

		await this.producer.connect();
		await this.consumer.connect();
		await this.admin.connect();
	}

	async disconnect(): Promise<void> {
		await this.producer.disconnect();
		await this.consumer.disconnect();
		await this.admin.disconnect();
	}

	private async subscribe(topic: string): Promise<void> {
		await this.consumer.subscribe({
			topic,
			fromBeginning: this.options.clientConfig.consumeFromBeginning || false,
		});
		this.logger.log('Subscribe: ' + topic);
	}

	async send(message: KafkaMessageSend): Promise<RecordMetadata[]> {
		if (!this.producer) {
			this.logger.error('There is no producer, unable to send message.');
			return;
		}
		this.logger.debug('Send message: ' + message);
		return this.producer.send({ topic: message.topic, messages: message.messages });
	}

	async sendWithReply<Request, Response>(topic: string, message: Request): Promise<Response> {
		return new Promise<Response>(async (resolve, reject) => {
			if (!this.producer) {
				this.logger.error('There is no producer, unable to send message.');
				reject('There is no producer, unable to send message.');
			}
			const headers: IHeaders = { correlationId: randomUUID(), requestTopic: this.responseTopic };
			this.responseEvents.once(headers.correlationId, (response) => {
				this.logger.debug('Reply message: ' + response);
				resolve(response);
			});
			await this.send({
				messages: [{ value: JSON.stringify(message), headers }],
				topic,
			});
		});
	}

	async commitOffsets(topicPartitions: Array<TopicPartitionOffsetAndMetadata>): Promise<void> {
		return this.consumer.commitOffsets(topicPartitions);
	}

	private bindAllTopicToConsumer(): void {
		const runConfig = this.options.clientConfig.consumerRunConfig ?? {};
		this.consumer.run({ ...runConfig, eachMessage: (message) => this.eachMessageController(message) });
	}

	async eachMessageController(payload: EachMessagePayload): Promise<void> {
		const headers: IHeaders = payload.message.headers as IHeaders;
		const message = JSON.parse(payload.message.value.toString());
		if (payload.topic === this.responseTopic) {
			this.responseEvents.emit(headers.correlationId, message);
			return;
		}
		const callback = SUBSCRIBER_MAP.get(payload.topic);
		const result = await callback(JSON.stringify(message.value));
		if (headers.requestTopic && headers.correlationId) {
			this.send({
				topic: headers.requestTopic,
				messages: [{ value: JSON.stringify(result), headers }],
			});
		}
	}
}
