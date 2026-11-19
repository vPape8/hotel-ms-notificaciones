import { Channel, Options } from 'amqplib';
import { colaDlq, colaReintento, colasDeTrabajo, rabbitConfig } from './rabbitmq.config';

// Declaración de exchanges, colas y bindings por caso de uso (equivalente a los @Bean Queue/Exchange/Binding).
//
//  Flujo del usuario · Reservas (topic: cada cola elige por routing key)
//    hotel-ms-reservas --reserva.creada / reserva.estado.<estado>--> hotel.reservas.exchange (topic)
//      ├─ reserva.creada   -> hotel.reservas.confirmacion.queue --> ConfirmacionReservaListener (correo "recibimos tu reserva")
//      ├─ reserva.estado.* -> hotel.reservas.estado.queue       --> EstadoReservaListener       (correo "tu reserva fue confirmada/cancelada…")
//      └─ reserva.#        -> hotel.reservas.registro.queue     --> RegistroReservaListener     (copia en MySQL para el informe PDF)
//
//  Flujo del admin · Informe (diagrama del profe)
//    hotel-ms-reservas --reporte.solicitado--> hotel.reportes.fanout (EX F)
//      ├─> hotel.reportes.pdf.queue   --> InformePdfListener --informe.generado (rk)--> hotel.reportes.direct (EX D)
//      │                                    --> hotel.reportes.mail.queue --> InformeMailListener (correo con el PDF)
//      └─> hotel.reportes.aviso.queue --> AvisoInformeListener (correo "se está generando"; falla con @falla.test)
//
//  Errores (cada cola con su propia DLQ)
//    recuperable -> hotel.reintentos.exchange --> <cola>.retry (TTL) --> vuelve a <cola>  (hasta N veces)
//    no recuperable o reintentos agotados -> NACK sin requeue -> hotel.dlx (rk = <cola>) --> <cola>.dlq --> ErrorListener
//    sin cola destino al publicar -> hotel.dlx (rk = mensaje.no-entregado) --> hotel.no-entregados.dlq --> ErrorListener

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

/**
 * Cola de trabajo con su cola de reintento y su DLQ.
 * x-dead-letter-routing-key = nombre de la cola: el DLX (direct) entrega cada mensaje muerto a la DLQ de su cola.
 */
function colaDeTrabajo(cola: string): { colas: DefinicionCola[]; bindings: DefinicionBinding[] } {
	return {
		colas: [
			{
				nombre: cola,
				argumentos: { 'x-dead-letter-exchange': exchanges.deadLetter, 'x-dead-letter-routing-key': cola }
			},
			{
				// Al vencer el TTL, el exchange por defecto ('') devuelve el mensaje a la cola original.
				nombre: colaReintento(cola),
				argumentos: { 'x-message-ttl': reintentos.esperaMs, 'x-dead-letter-exchange': '', 'x-dead-letter-routing-key': cola }
			},
			{ nombre: colaDlq(cola) }
		],
		bindings: [
			{ exchange: exchanges.reintentos, cola: colaReintento(cola), routingKey: cola },
			{ exchange: exchanges.deadLetter, cola: colaDlq(cola), routingKey: cola }
		]
	};
}

const definicionesDeTrabajo = colasDeTrabajo.map(colaDeTrabajo);

/** Todas las DLQ que lee el ErrorListener ("listener error" del diagrama). */
export const colasDlq: string[] = [...colasDeTrabajo.map(colaDlq), queues.noEntregados];

export const topologiaNotificaciones = {
	exchanges: [
		{ nombre: exchanges.reservas, tipo: 'topic' },
		{ nombre: exchanges.reportesFanout, tipo: 'fanout' },
		{ nombre: exchanges.reportesDirect, tipo: 'direct' },
		{ nombre: exchanges.reintentos, tipo: 'direct' },
		{ nombre: exchanges.deadLetter, tipo: 'direct' }
	] satisfies DefinicionExchange[],

	colas: [
		...definicionesDeTrabajo.flatMap((cola) => cola.colas),
		{ nombre: queues.noEntregados }
	] satisfies DefinicionCola[],

	bindings: [
		// Usuario: el topic reparte por patrón de routing key
		{ exchange: exchanges.reservas, cola: queues.confirmacion, routingKey: routingKeys.reservaCreada },
		{ exchange: exchanges.reservas, cola: queues.estado, routingKey: routingKeys.reservaEstado },
		{ exchange: exchanges.reservas, cola: queues.registro, routingKey: routingKeys.reservaTodas },
		// Admin: el fanout ignora la routing key y copia el mensaje a las dos colas
		{ exchange: exchanges.reportesFanout, cola: queues.informePdf, routingKey: '' },
		{ exchange: exchanges.reportesFanout, cola: queues.informeAviso, routingKey: '' },
		{ exchange: exchanges.reportesDirect, cola: queues.informeMail, routingKey: routingKeys.informeGenerado },
		// Reintentos y DLQ de cada cola
		...definicionesDeTrabajo.flatMap((cola) => cola.bindings),
		// Devueltos al productor por no tener cola destino
		{ exchange: exchanges.deadLetter, cola: queues.noEntregados, routingKey: routingKeys.noEntregado }
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
