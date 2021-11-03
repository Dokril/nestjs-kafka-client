import { Module, DynamicModule, Global, Provider } from '@nestjs/common';
import { KafkaService } from './kafka.service';
import { KafkaModuleOption, KafkaModuleOptionsAsync, KafkaOptionsFactory } from './interfaces';
import { KafkaModuleOptionsProvider } from './kafka-module-options.provider';
import { KAFKA_MODULE_OPTIONS } from './constants';

@Global()
@Module({})
export class KafkaModule {
	static forRoot(options: KafkaModuleOption[]): DynamicModule {
		const clients = options.map((item) => ({
			provide: item.name,
			useValue: new KafkaService(item),
		}));

		return {
			module: KafkaModule,
			providers: clients,
			exports: clients,
		};
	}

	public static forRootAsync(consumers: string[], connectOptions: KafkaModuleOptionsAsync): DynamicModule {
		const clients = [];
		for (const consumer of consumers) {
			clients.push({
				provide: consumer,
				useFactory: async (optionsProvider: KafkaModuleOptionsProvider) => {
					return new KafkaService(optionsProvider.getOptionsByName(consumer));
				},
				inject: [KafkaModuleOptionsProvider],
			});
		}

		const createKafkaModuleOptionsProvider = this.createAsyncOptionsProvider(connectOptions);

		return {
			module: KafkaModule,
			imports: connectOptions.imports || [],
			providers: [createKafkaModuleOptionsProvider, KafkaModuleOptionsProvider, ...clients],
			exports: [createKafkaModuleOptionsProvider, ...clients],
		};
	}

	private static createAsyncOptionsProvider(options: KafkaModuleOptionsAsync): Provider {
		if (options.useFactory) {
			return {
				provide: KAFKA_MODULE_OPTIONS,
				useFactory: options.useFactory,
				inject: options.inject || [],
			};
		}
		return {
			provide: KAFKA_MODULE_OPTIONS,
			useFactory: async (optionsFactory: KafkaOptionsFactory) => optionsFactory.creatKafkaModuleOptions(),
			inject: [options.useExisting || options.useClass],
		};
	}
}
