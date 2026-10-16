// Configuración centralizada de RabbitMQ (equivalente a application.yml / clase @Configuration).
// Los consumidores, la topología y el publicador leen los nombres solo desde aquí.
// Deben coincidir con hotel-ms-reservas (exchange de reservas y DLX) y con hotel-ms-rabbitmq-admin.

function numero(variable: string | undefined, porDefecto: number): number {
	const valor = Number(variable);
	return Number.isFinite(valor) && valor > 0 ? valor : porDefecto;
}

export const rabbitConfig = {
	url: process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672',
	reconexionMs: numero(process.env.RABBITMQ_RECONEXION_MS, 5000),
	/** Mensajes sin ACK que cada consumidor puede tener a la vez. */
	prefetch: numero(process.env.RABBITMQ_PREFETCH, 5),

	reintentos: {
		/** Reintentos ante errores recuperables antes de mandar el mensaje a la DLQ. */
		maximo: numero(process.env.RABBITMQ_MAX_REINTENTOS, 3),
		/** Tiempo que el mensaje espera en la cola de reintento (TTL) antes de volver a su cola. */
		esperaMs: numero(process.env.RABBITMQ_ESPERA_REINTENTO_MS, 5000),
		header: 'x-reintentos'
	},

	exchanges: {
		/** Topic donde hotel-ms-reservas publica reserva.creada y reporte.solicitado. */
		reservas: process.env.RABBITMQ_EXCHANGE_RESERVAS ?? 'hotel.reservas.exchange',
		/** Direct donde este servicio publica los PDF generados (paso "GEN PDF -> EX -> Q" del diagrama). */
		documentos: process.env.RABBITMQ_EXCHANGE_DOCUMENTOS ?? 'hotel.documentos.exchange',
		/** Direct que deriva cada mensaje a la cola de reintento de su cola original. */
		reintentos: process.env.RABBITMQ_EXCHANGE_REINTENTOS ?? 'hotel.reintentos.exchange',
		/** Direct: dead-letter exchange compartido. */
		deadLetter: process.env.RABBITMQ_EXCHANGE_DLX ?? 'hotel.dlx'
	},

	queues: {
		/** Cola 1: confirmación de la reserva al huésped. */
		confirmacion: 'hotel.reservas.confirmacion.queue',
		/** Cola 2: informes para administración (registra reservas y atiende solicitudes de informe). */
		reportes: 'hotel.reportes.queue',
		/** Envío de los PDF generados por las dos colas anteriores. */
		notificaciones: 'hotel.notificaciones.queue',

		confirmacionDlq: 'hotel.reservas.confirmacion.dlq',
		reportesDlq: 'hotel.reportes.dlq',
		notificacionesDlq: 'hotel.notificaciones.dlq',
		/** La declara hotel-ms-reservas: mensajes que el broker no pudo enrutar al publicarlos. */
		noEntregadosDlq: 'hotel.no-entregados.dlq'
	},

	routingKeys: {
		reservaCreada: 'reserva.creada',
		reporteSolicitado: 'reporte.solicitado',
		documentoGenerado: 'documento.generado',

		confirmacionDead: 'confirmacion.dead',
		reportesDead: 'reportes.dead',
		notificacionesDead: 'notificaciones.dead',
		noEntregado: 'mensaje.no-entregado'
	}
} as const;

/** Las colas de reintento se nombran a partir de su cola original. */
export function colaReintento(cola: string): string {
	return `${cola}.retry`;
}
