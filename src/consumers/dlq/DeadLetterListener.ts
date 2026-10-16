import { rabbitConfig } from '../../config/rabbitmq.config';
import { log, registrarEnArchivoDlq } from '../../logs/logger';
import { ContextoMensaje, RabbitListener } from '../../messaging/RabbitListener';

interface RegistroXDeath {
	queue?: string;
	reason?: string;
	count?: number;
	exchange?: string;
	'routing-keys'?: string[];
}

/**
 * Consumidores de las DLQ: registran cada mensaje muerto en el log (consola + logs/dlq.log) con el motivo
 * que deja RabbitMQ en el header x-death, y hacen ACK. reintentar=false: un mensaje muerto no se reprocesa solo.
 */
export class DeadLetterListener {
	@RabbitListener({ cola: rabbitConfig.queues.confirmacionDlq, reintentar: false })
	async confirmacion(cuerpo: unknown, contexto: ContextoMensaje): Promise<void> {
		await this.registrar(cuerpo, contexto);
	}

	@RabbitListener({ cola: rabbitConfig.queues.reportesDlq, reintentar: false })
	async reportes(cuerpo: unknown, contexto: ContextoMensaje): Promise<void> {
		await this.registrar(cuerpo, contexto);
	}

	@RabbitListener({ cola: rabbitConfig.queues.notificacionesDlq, reintentar: false })
	async notificaciones(cuerpo: unknown, contexto: ContextoMensaje): Promise<void> {
		await this.registrar(cuerpo, contexto);
	}

	@RabbitListener({ cola: rabbitConfig.queues.noEntregadosDlq, reintentar: false })
	async noEntregados(cuerpo: unknown, contexto: ContextoMensaje): Promise<void> {
		await this.registrar(cuerpo, contexto);
	}

	private async registrar(cuerpo: unknown, contexto: ContextoMensaje): Promise<void> {
		const xDeath = (contexto.headers['x-death'] as RegistroXDeath[] | undefined)?.[0];
		const evento = (typeof cuerpo === 'object' && cuerpo !== null ? cuerpo : {}) as { id?: unknown; tipo?: unknown };
		const registro = {
			dlq: contexto.cola,
			messageId: contexto.messageId,
			eventoId: evento.id,
			tipo: evento.tipo,
			reintentos: contexto.reintentos,
			colaOriginal: xDeath?.queue ?? contexto.headers['x-exchange-original'],
			motivo: xDeath?.reason ?? contexto.headers['x-motivo'],
			routingKeyOriginal: xDeath?.['routing-keys']?.[0] ?? contexto.headers['x-routing-key-original']
		};
		log('error', 'MENSAJE_MUERTO', registro);
		await registrarEnArchivoDlq({ ...registro, cuerpo }).catch((error: unknown) =>
			log('warn', 'DLQ_LOG_ARCHIVO_FALLIDO', { motivo: error instanceof Error ? error.message : String(error) }));
	}
}
