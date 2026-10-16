import { Channel, Options } from 'amqplib';
import { colaReintento, rabbitConfig } from './rabbitmq.config';

// Declaración de exchanges, colas y bindings por caso de uso (equivalente a los @Bean Queue/Exchange/Binding).
// Rutas de mensajería:
//
//  Cola 1 · Confirmación de reserva
//    hotel-ms-reservas --reserva.creada--> hotel.reservas.exchange (topic) --> hotel.reservas.confirmacion.queue
//      --> ConfirmacionReservaListener genera el PDF --documento.generado--> hotel.documentos.exchange (direct)
//      --> hotel.notificaciones.queue --> NotificacionListener guarda el PDF y lo envía por correo
//
//  Cola 2 · Informes para administración
//    hotel.reservas.exchange --reserva.creada------> hotel.reportes.queue (registra la reserva para el informe)
//    hotel.reservas.exchange --reporte.solicitado--> hotel.reportes.queue (arma el informe PDF)
//      --> documento.generado --> hotel.documentos.exchange --> hotel.notificaciones.queue --> correo al admin
//
//  Errores (en todas las colas)
//    recuperable    -> hotel.reintentos.exchange --> <cola>.retry (TTL) --> vuelve a <cola>  (hasta N veces)
//    no recuperable o reintentos agotados -> nack sin requeue -> hotel.dlx --> <cola>.dlq --> DeadLetterListener (log)

export interface DefinicionExchange {
	nombre: string;
	tipo: 'direct' | 'topic' | 'fanout';
}

export interface DefinicionCola {
	nombre: string;
	argumentos?: Options.AssertQueue['arguments'];
}

export interface DefinicionBinding {
	exchange: string;
	cola: string;
	routingKey: string;
}

const { exchanges, queues, routingKeys, reintentos } = rabbitConfig;

/** Cola de trabajo + su cola de reintento + su DLQ, todas enlazadas. */
function colaConReintentoYDlq(cola: string, dlq: string, routingKeyDead: string): {
	colas: DefinicionCola[];
	bindings: DefinicionBinding[];
} {
	return {
		colas: [
			{
				nombre: cola,
				argumentos: { 'x-dead-letter-exchange': exchanges.deadLetter, 'x-dead-letter-routing-key': routingKeyDead }
			},
			{
				// Al vencer el TTL, el exchange por defecto ('') devuelve el mensaje a la cola original.
				nombre: colaReintento(cola),
				argumentos: { 'x-message-ttl': reintentos.esperaMs, 'x-dead-letter-exchange': '', 'x-dead-letter-routing-key': cola }
			},
			{ nombre: dlq }
		],
		bindings: [
			{ exchange: exchanges.reintentos, cola: colaReintento(cola), routingKey: cola },
			{ exchange: exchanges.deadLetter, cola: dlq, routingKey: routingKeyDead }
		]
	};
}

const confirmacion = colaConReintentoYDlq(queues.confirmacion, queues.confirmacionDlq, routingKeys.confirmacionDead);
const reportes = colaConReintentoYDlq(queues.reportes, queues.reportesDlq, routingKeys.reportesDead);
const notificaciones = colaConReintentoYDlq(queues.notificaciones, queues.notificacionesDlq, routingKeys.notificacionesDead);

export const topologiaNotificaciones = {
	exchanges: [
		{ nombre: exchanges.reservas, tipo: 'topic' },
		{ nombre: exchanges.documentos, tipo: 'direct' },
		{ nombre: exchanges.reintentos, tipo: 'direct' },
		{ nombre: exchanges.deadLetter, tipo: 'direct' }
	] satisfies DefinicionExchange[],

	colas: [
		...confirmacion.colas,
		...reportes.colas,
		...notificaciones.colas,
		{ nombre: queues.noEntregadosDlq }
	] satisfies DefinicionCola[],

	bindings: [
		// Cola 1: confirmación
		{ exchange: exchanges.reservas, cola: queues.confirmacion, routingKey: routingKeys.reservaCreada },
		...confirmacion.bindings,
		// Cola 2: informes
		{ exchange: exchanges.reservas, cola: queues.reportes, routingKey: routingKeys.reservaCreada },
		{ exchange: exchanges.reservas, cola: queues.reportes, routingKey: routingKeys.reporteSolicitado },
		...reportes.bindings,
		// Envío de documentos generados
		{ exchange: exchanges.documentos, cola: queues.notificaciones, routingKey: routingKeys.documentoGenerado },
		...notificaciones.bindings,
		// Mensajes no entregados por el productor
		{ exchange: exchanges.deadLetter, cola: queues.noEntregadosDlq, routingKey: routingKeys.noEntregado }
	] satisfies DefinicionBinding[]
};

export async function declararTopologia(canal: Channel): Promise<void> {
	for (const exchange of topologiaNotificaciones.exchanges) {
		await canal.assertExchange(exchange.nombre, exchange.tipo, { durable: true });
	}
	for (const cola of topologiaNotificaciones.colas) {
		await canal.assertQueue(cola.nombre, { durable: true, arguments: cola.argumentos });
	}
	for (const binding of topologiaNotificaciones.bindings) {
		await canal.bindQueue(binding.cola, binding.exchange, binding.routingKey);
	}
}
