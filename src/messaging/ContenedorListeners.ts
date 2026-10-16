import amqp, { Channel, ConfirmChannel, ConsumeMessage, Options } from 'amqplib';
import { rabbitConfig } from '../config/rabbitmq.config';
import { declararTopologia } from '../config/rabbitmq.topology';
import { log } from '../logs/logger';
import { OperacionesMensaje, procesarMensaje } from './procesarMensaje';
import { ListenerRegistrado } from './RabbitListener';

/** Lo que necesitan los listeners para publicar (p. ej. el PDF generado), sin conocer amqplib. */
export interface PublicadorMensajes {
	publicar(exchange: string, routingKey: string, cuerpo: unknown, opciones?: Options.Publish): Promise<void>;
}

/**
 * Mantiene la conexión con RabbitMQ: declara la topología, suscribe cada @RabbitListener
 * y se reconecta solo si el broker se cae. Usa un canal para consumir (ACK manual + prefetch)
 * y un canal con confirmaciones para publicar.
 */
export class ContenedorListeners implements PublicadorMensajes {
	private conexion?: amqp.ChannelModel;
	private canalPublicacion?: ConfirmChannel;
	private detenido = false;
	private conectado = false;

	constructor(private readonly listeners: () => readonly ListenerRegistrado[]) {}

	get estaConectado(): boolean {
		return this.conectado;
	}

	async iniciar(): Promise<void> {
		this.detenido = false;
		try {
			await this.conectar();
		} catch (error) {
			log('error', 'CONEXION_FALLIDA', { motivo: error instanceof Error ? error.message : String(error), reintentoEnMs: rabbitConfig.reconexionMs });
			this.programarReconexion();
		}
	}

	async detener(): Promise<void> {
		this.detenido = true;
		const conexion = this.conexion;
		this.conexion = undefined;
		await conexion?.close().catch(() => undefined);
	}

	async publicar(exchange: string, routingKey: string, cuerpo: unknown, opciones: Options.Publish = {}): Promise<void> {
		const canal = this.canalPublicacion;
		if (!canal) {
			throw new Error('Sin conexión con RabbitMQ');
		}
		const contenido = Buffer.isBuffer(cuerpo) ? cuerpo : Buffer.from(JSON.stringify(cuerpo));
		await new Promise<void>((resolve, reject) => {
			canal.publish(exchange, routingKey, contenido, {
				persistent: true,
				contentType: 'application/json',
				timestamp: Date.now(),
				appId: 'hotel-ms-notificaciones',
				...opciones
			}, (error) => (error ? reject(error) : resolve()));
		});
	}

	private async conectar(): Promise<void> {
		const conexion = await amqp.connect(rabbitConfig.url);
		conexion.on('error', (error: Error) => log('error', 'CONEXION_ERROR', { motivo: error.message }));
		conexion.on('close', () => {
			this.conectado = false;
			this.conexion = undefined;
			this.canalPublicacion = undefined;
			if (!this.detenido) {
				log('warn', 'CONEXION_CERRADA', { reintentoEnMs: rabbitConfig.reconexionMs });
				this.programarReconexion();
			}
		});

		try {
			const canalPublicacion = await conexion.createConfirmChannel();
			await declararTopologia(canalPublicacion);

			const canalConsumo = await conexion.createChannel();
			await canalConsumo.prefetch(rabbitConfig.prefetch);
			for (const canal of [canalPublicacion, canalConsumo]) {
				canal.on('error', (error: Error) => log('error', 'CANAL_ERROR', { motivo: error.message }));
			}

			this.conexion = conexion;
			this.canalPublicacion = canalPublicacion;

			for (const listener of this.listeners()) {
				await this.suscribir(canalConsumo, listener);
			}
		} catch (error) {
			// Conexión a medio configurar (p. ej. una cola ya existe con otros argumentos): se descarta entera.
			conexion.removeAllListeners('close');
			this.conexion = undefined;
			this.canalPublicacion = undefined;
			await conexion.close().catch(() => undefined);
			throw error;
		}
		this.conectado = true;
		log('info', 'CONEXION_ESTABLECIDA', { listeners: this.listeners().map((listener) => `${listener.nombre} <- ${listener.cola}`) });
	}

	private async suscribir(canal: Channel, listener: ListenerRegistrado): Promise<void> {
		const operaciones: OperacionesMensaje = {
			ack: (mensaje) => canal.ack(mensaje),
			nack: (mensaje, requeue) => canal.nack(mensaje, false, requeue),
			reintentar: (mensaje, cola, reintentos) => this.publicar(rabbitConfig.exchanges.reintentos, cola, mensaje.content, {
				...mensaje.properties,
				headers: { ...mensaje.properties.headers, [rabbitConfig.reintentos.header]: reintentos }
			})
		};

		await canal.consume(listener.cola, (mensaje: ConsumeMessage | null) => {
			if (!mensaje) {
				log('warn', 'CONSUMIDOR_CANCELADO', { listener: listener.nombre, cola: listener.cola });
				return;
			}
			void procesarMensaje(mensaje, listener, operaciones);
		}, { noAck: false });
	}

	private programarReconexion(): void {
		if (this.detenido) {
			return;
		}
		setTimeout(() => void this.iniciar(), rabbitConfig.reconexionMs);
	}
}
