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
 * "listener error" del diagrama: consume la única DLQ (hotel.dlq), registra cada mensaje muerto
 * en consola y en logs/dlq.log con la cola de origen y el motivo (header x-death) y hace ACK.
 * reintentar=false: un mensaje muerto no se vuelve a procesar solo.
 */
export class ErrorListener {
	@RabbitListener({ cola: rabbitConfig.queues.dlq, reintentar: false })
	async registrarError(cuerpo: unknown, contexto: ContextoMensaje): Promise<void> {
		const xDeath = (contexto.headers['x-death'] as RegistroXDeath[] | undefined)?.[0];
		const evento = (typeof cuerpo === 'object' && cuerpo !== null ? cuerpo : {}) as { id?: unknown; tipo?: unknown };
		const registro = {
			dlq: contexto.cola,
			messageId: contexto.messageId,
			eventoId: evento.id,
			tipo: evento.tipo,
			reintentos: contexto.reintentos,
			// Mensajes rechazados por un consumidor (x-death) o devueltos al productor (x-motivo).
			colaOrigen: xDeath?.queue ?? contexto.headers['x-exchange-original'],
			motivo: xDeath?.reason ?? contexto.headers['x-motivo'],
			routingKeyOriginal: xDeath?.['routing-keys']?.[0] ?? contexto.headers['x-routing-key-original']
		};
		log('error', 'MENSAJE_MUERTO', registro);
		await registrarEnArchivoDlq({ ...registro, cuerpo }).catch((error: unknown) =>
			log('warn', 'DLQ_LOG_ARCHIVO_FALLIDO', { motivo: error instanceof Error ? error.message : String(error) }));
	}
}
