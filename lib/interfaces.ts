import {
	ConsumerConfig,
	KafkaConfig,
	ProducerConfig,
	ProducerRecord,
	Message,
	ConsumerRunConfig,
	Transaction,
	RecordMetadata,
} from 'kafkajs';
import { LoggerService, ModuleMetadata, Type } from '@nestjs/common';

export interface IHeaders extends Partial<KafkaHeaders>, Record<string, string> {}

export interface KafkaHeaders {
	correlationId: string;
	requestTopic: string;
	partision: string;
}

export interface KafkaResponse<T = any> {
	response: T;
	key: string;
	timestamp: string;
	offset: number;
	headers?: IHeaders;
}
export interface KafkaModuleOption {
	name: string;
	clientConfig: {
		client: KafkaConfig;
		consumer: ConsumerConfig;
		consumerRunConfig?: ConsumerRunConfig;
		producer?: ProducerConfig;
		consumeFromBeginning?: boolean;
		seek?: Record<string, number | 'earliest' | Date>;
		autoConnect?: boolean;
	};
	logger?: LoggerService;
	replayMode?: boolean;
}

export interface KafkaMessageObject extends Message {
	value: any | Buffer | string | null;
	key?: any;
}

export interface KafkaMessageSend extends Omit<ProducerRecord, 'topic'> {
	messages: KafkaMessageObject[];
	topic: string;
}

export interface KafkaModuleOptionsAsync extends Pick<ModuleMetadata, 'imports'> {
	inject?: any[];
	useExisting?: Type<KafkaOptionsFactory>;
	useClass?: Type<KafkaOptionsFactory>;
	useFactory?: (...args: any[]) => Promise<KafkaModuleOption[]> | KafkaModuleOption[];
}

export interface KafkaOptionsFactory {
	creatKafkaModuleOptions(): Promise<KafkaModuleOption[]> | KafkaModuleOption[];
}

export interface KafkaTransaction extends Omit<Transaction, 'send' | 'sendBatch'> {
	send(message: KafkaMessageSend): Promise<RecordMetadata[]>;
}
