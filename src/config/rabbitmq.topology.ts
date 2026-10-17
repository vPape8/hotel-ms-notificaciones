import { Channel, Options } from 'amqplib';
import { colaReintento, rabbitConfig } from './rabbitmq.config';

// Declaración de exchanges, colas y bindings por caso de uso (equivalente a los @Bean Queue/Exchange/Binding).
//
//  Flujo del usuario · Confirmación de reserva
//    hotel-ms-reservas --reserva.creada--> hotel.reservas.exchange (topic)
//      ├─> hotel.reservas.confirmacion.queue --> ConfirmacionReservaListener (correo al huésped con el comprobante)
//      └─> hotel.reservas.registro.queue     --> RegistroReservaListener     (copia en MySQL para los informes)
//
//  Flujo del admin · Informe (diagrama del profe)
//    hotel-ms-reservas --reporte.solicitado--> hotel.reportes.fanout (EX F)
//      ├─> hotel.reportes.pdf.queue   --> InformePdfListener --informe.generado (rk)--> hotel.reportes.direct (EX D)
//      │                                    --> hotel.reportes.mail.queue --> InformeMailListener (correo con el PDF)
//      └─> hotel.reportes.aviso.queue --> AvisoInformeListener (correo "se está generando"; falla con @falla.test)
//
//  Errores (todas las colas)
//    recuperable -> hotel.reintentos.exchange --> <cola>.retry (TTL) --> vuelve a <cola>  (hasta N veces)
//    no recuperable o reintentos agotados -> NACK sin requeue -> hotel.dlx (dlx) --> hotel.dlq (dlq) --> ErrorListener

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

/** Cola de trabajo con su DLX y su cola de reintento. */
function colaDeTrabajo(cola: string): { colas: DefinicionCola[]; bindings: DefinicionBinding[] } {
	return {
		colas: [
			{
				nombre: cola,
				argumentos: { 'x-dead-letter-exchange': exchanges.deadLetter, 'x-dead-letter-routing-key': routingKeys.mensajeMuerto }
			},
			{
				// Al vencer el TTL, el exchange por defecto ('') devuelve el mensaje a la cola original.
				nombre: colaReintento(cola),
				argumentos: { 'x-message-ttl': reintentos.esperaMs, 'x-dead-letter-exchange': '', 'x-dead-letter-routing-key': cola }
			}
		],
		bindings: [{ exchange: exchanges.reintentos, cola: colaReintento(cola), routingKey: cola }]
	};
}

const colasDeTrabajo = [queues.confirmacion, queues.registro, queues.informePdf, queues.informeAviso, queues.informeMail].map(colaDeTrabajo);

export const topologiaNotificaciones = {
	exchanges: [
		{ nombre: exchanges.reservas, tipo: 'topic' },
		{ nombre: exchanges.reportesFanout, tipo: 'fanout' },
		{ nombre: exchanges.reportesDirect, tipo: 'direct' },
		{ nombre: exchanges.reintentos, tipo: 'direct' },
		{ nombre: exchanges.deadLetter, tipo: 'direct' }
	] satisfies DefinicionExchange[],

	colas: [
		...colasDeTrabajo.flatMap((cola) => cola.colas),
		{ nombre: queues.dlq }
	] satisfies DefinicionCola[],

	bindings: [
		// Usuario: confirmación + registro para informes
		{ exchange: exchanges.reservas, cola: queues.confirmacion, routingKey: routingKeys.reservaCreada },
		{ exchange: exchanges.reservas, cola: queues.registro, routingKey: routingKeys.reservaCreada },
		// Admin: el fanout ignora la routing key y copia el mensaje a las dos colas
		{ exchange: exchanges.reportesFanout, cola: queues.informePdf, routingKey: '' },
		{ exchange: exchanges.reportesFanout, cola: queues.informeAviso, routingKey: '' },
		{ exchange: exchanges.reportesDirect, cola: queues.informeMail, routingKey: routingKeys.informeGenerado },
		// Reintentos
		...colasDeTrabajo.flatMap((cola) => cola.bindings),
		// DLX -> DLQ
		{ exchange: exchanges.deadLetter, cola: queues.dlq, routingKey: routingKeys.mensajeMuerto },
		{ exchange: exchanges.deadLetter, cola: queues.dlq, routingKey: routingKeys.noEntregado }
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
