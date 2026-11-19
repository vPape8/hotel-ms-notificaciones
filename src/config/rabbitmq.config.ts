// Configuración centralizada de RabbitMQ (equivalente a application.yml / clase @Configuration).
// Los consumidores, la topología y los publicadores leen los nombres solo desde aquí.
// Deben coincidir con hotel-ms-reservas (productor) y con hotel-ms-rabbitmq-admin (recursos protegidos).

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
		/** Topic donde hotel-ms-reservas publica reserva.creada y reserva.estado.<estado> (flujo del usuario). */
		reservas: process.env.RABBITMQ_EXCHANGE_RESERVAS ?? 'hotel.reservas.exchange',
		/** "EX F" del diagrama: fanout donde hotel-ms-reservas publica reporte.solicitado (flujo del admin). */
		reportesFanout: process.env.RABBITMQ_EXCHANGE_REPORTES_FANOUT ?? 'hotel.reportes.fanout',
		/** "EX D" del diagrama: direct donde el listener PDF publica el informe generado. */
		reportesDirect: process.env.RABBITMQ_EXCHANGE_REPORTES_DIRECT ?? 'hotel.reportes.direct',
		/** Direct que deriva cada mensaje a la cola de reintento de su cola original. */
		reintentos: process.env.RABBITMQ_EXCHANGE_REINTENTOS ?? 'hotel.reintentos.exchange',
		/** "dlx" del diagrama: dead-letter exchange (direct). La routing key es el nombre de la cola, así cada una llega a su DLQ. */
		deadLetter: process.env.RABBITMQ_EXCHANGE_DLX ?? 'hotel.dlx'
	},

	queues: {
		// Flujo del usuario: confirmación de la reserva por correo.
		confirmacion: 'hotel.reservas.confirmacion.queue',
		/** Aviso al huésped cuando su reserva cambia de estado (confirmada, cancelada o completada). */
		estado: 'hotel.reservas.estado.queue',
		/** Copia de cada reserva (y de sus cambios de estado) en MySQL para calcular el informe PDF del admin. */
		registro: 'hotel.reservas.registro.queue',

		// Flujo del admin (diagrama del profe): informe de reservas.
		/** Cola del fanout -> listener pdf. */
		informePdf: 'hotel.reportes.pdf.queue',
		/** Cola del fanout -> listener mail (aviso "tu informe se está generando"). Es la que falla en la demo. */
		informeAviso: 'hotel.reportes.aviso.queue',
		/** Cola del exchange directo -> listener mail que envía el PDF. */
		informeMail: 'hotel.reportes.mail.queue',

		/** Lo que hotel-ms-reservas publicó y el broker devolvió por no tener cola destino (no pertenece a ninguna cola). */
		noEntregados: 'hotel.no-entregados.dlq'
	},

	routingKeys: {
		reservaCreada: 'reserva.creada',
		/** hotel-ms-reservas publica reserva.estado.confirmada, reserva.estado.cancelada o reserva.estado.completada. */
		reservaEstado: 'reserva.estado.*',
		/** Patrón topic que recibe la creación y todos los cambios de estado. */
		reservaTodas: 'reserva.#',
		/** Routing key del exchange directo (flecha "rk" del diagrama). */
		informeGenerado: 'informe.generado',
		/** El productor manda aquí lo que el broker le devolvió por no tener cola destino. */
		noEntregado: 'mensaje.no-entregado'
	},

	/** Falla simulada para la demo: el aviso al admin falla si su email termina con este dominio. */
	falloSimulado: {
		dominio: process.env.SIMULAR_FALLO_DOMINIO ?? '@falla.test'
	}
} as const;

/** Las colas de reintento se nombran a partir de su cola original. */
export function colaReintento(cola: string): string {
	return `${cola}.retry`;
}

/** Cada cola de trabajo tiene su propia DLQ con el mismo nombre más ".dlq". */
export function colaDlq(cola: string): string {
	return `${cola}.dlq`;
}

/** Colas de trabajo: cada una con su cola de reintento y su DLQ. */
export const colasDeTrabajo = [
	rabbitConfig.queues.confirmacion,
	rabbitConfig.queues.estado,
	rabbitConfig.queues.registro,
	rabbitConfig.queues.informePdf,
	rabbitConfig.queues.informeAviso,
	rabbitConfig.queues.informeMail
] as const;
