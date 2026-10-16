import { ConsumeMessage } from 'amqplib';
import { rabbitConfig } from '../config/rabbitmq.config';
import { log } from '../logs/logger';
import { ErrorNoRecuperable } from './errores';
import { ContextoMensaje, ListenerRegistrado } from './RabbitListener';

/** Operaciones sobre el broker que necesita el procesamiento (en los tests se reemplazan por dobles). */
export interface OperacionesMensaje {
	ack(mensaje: ConsumeMessage): void;
	/** requeue=false manda el mensaje al dead-letter exchange de su cola. */
	nack(mensaje: ConsumeMessage, requeue: boolean): void;
	/** Publica una copia en la cola de reintento (con el contador incrementado). */
	reintentar(mensaje: ConsumeMessage, cola: string, reintentos: number): Promise<void>;
}

export type Resultado = 'ACK' | 'REINTENTO' | 'DLQ' | 'REQUEUE';

function contadorReintentos(mensaje: ConsumeMessage): number {
	const valor = Number(mensaje.properties.headers?.[rabbitConfig.reintentos.header] ?? 0);
	return Number.isInteger(valor) && valor >= 0 ? valor : 0;
}

function leerJson(mensaje: ConsumeMessage, listener: ListenerRegistrado): unknown {
	const texto = mensaje.content.toString('utf8');
	try {
		return JSON.parse(texto);
	} catch {
		// En las DLQ (sin reintentos) igual se quiere registrar el mensaje, aunque no sea JSON.
		if (!listener.reintentar) {
			return texto;
		}
		throw new ErrorNoRecuperable('El cuerpo del mensaje no es JSON válido');
	}
}

/**
 * Decide qué hacer con cada mensaje (ACK manual, nunca automático):
 *  - éxito                               -> ACK
 *  - ErrorNoRecuperable                  -> NACK sin requeue -> DLQ
 *  - error recuperable con reintentos    -> copia a <cola>.retry (espera TTL y vuelve) + ACK del original
 *  - error recuperable sin reintentos    -> NACK sin requeue -> DLQ
 *  - no se pudo programar el reintento   -> NACK con requeue (el broker lo vuelve a entregar)
 */
export async function procesarMensaje(
	mensaje: ConsumeMessage,
	listener: ListenerRegistrado,
	operaciones: OperacionesMensaje
): Promise<Resultado> {
	const reintentos = contadorReintentos(mensaje);
	const contexto: ContextoMensaje = {
		cola: listener.cola,
		routingKey: mensaje.fields.routingKey,
		messageId: mensaje.properties.messageId,
		reintentos,
		headers: mensaje.properties.headers ?? {}
	};
	const datosLog = { listener: listener.nombre, cola: listener.cola, messageId: contexto.messageId, reintentos };

	try {
		const datos = listener.validar(leerJson(mensaje, listener));
		await listener.manejar(datos, contexto);
		operaciones.ack(mensaje);
		log('info', 'MENSAJE_PROCESADO', datosLog);
		return 'ACK';
	} catch (error) {
		const motivo = error instanceof Error ? error.message : String(error);
		const recuperable = !(error instanceof ErrorNoRecuperable);

		if (!recuperable || !listener.reintentar) {
			operaciones.nack(mensaje, false);
			log('error', 'MENSAJE_A_DLQ', { ...datosLog, motivo, recuperable });
			return 'DLQ';
		}

		if (reintentos >= rabbitConfig.reintentos.maximo) {
			operaciones.nack(mensaje, false);
			log('error', 'REINTENTOS_AGOTADOS', { ...datosLog, motivo, maximo: rabbitConfig.reintentos.maximo });
			return 'DLQ';
		}

		try {
			await operaciones.reintentar(mensaje, listener.cola, reintentos + 1);
			operaciones.ack(mensaje);
			log('warn', 'MENSAJE_REINTENTO', { ...datosLog, motivo, proximoIntento: reintentos + 1, esperaMs: rabbitConfig.reintentos.esperaMs });
			return 'REINTENTO';
		} catch (errorReintento) {
			operaciones.nack(mensaje, true);
			log('error', 'REINTENTO_NO_PROGRAMADO', {
				...datosLog,
				motivo,
				detalle: errorReintento instanceof Error ? errorReintento.message : String(errorReintento)
			});
			return 'REQUEUE';
		}
	}
}
