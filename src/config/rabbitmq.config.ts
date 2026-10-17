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
		/** Topic donde hotel-ms-reservas publica reserva.creada (flujo del usuario). */
		reservas: process.env.RABBITMQ_EXCHANGE_RESERVAS ?? 'hotel.reservas.exchange',
		/** "EX F" del diagrama: fanout donde hotel-ms-reservas publica reporte.solicitado (flujo del admin). */
		reportesFanout: process.env.RABBITMQ_EXCHANGE_REPORTES_FANOUT ?? 'hotel.reportes.fanout',
		/** "EX D" del diagrama: direct donde el listener PDF publica el informe generado. */
		reportesDirect: process.env.RABBITMQ_EXCHANGE_REPORTES_DIRECT ?? 'hotel.reportes.direct',
		/** Direct que deriva cada mensaje a la cola de reintento de su cola original. */
		reintentos: process.env.RABBITMQ_EXCHANGE_REINTENTOS ?? 'hotel.reintentos.exchange',
		/** "dlx" del diagrama: dead-letter exchange compartido por todas las colas. */
		deadLetter: process.env.RABBITMQ_EXCHANGE_DLX ?? 'hotel.dlx'
	},

	queues: {
		// Flujo del usuario: confirmación de la reserva por correo.
		confirmacion: 'hotel.reservas.confirmacion.queue',
		/** Guarda una copia de cada reserva en MySQL para poder calcular los informes del admin. */
		registro: 'hotel.reservas.registro.queue',

		// Flujo del admin (diagrama del profe): informe de reservas.
		/** Cola del fanout -> listener pdf. */
		informePdf: 'hotel.reportes.pdf.queue',
		/** Cola del fanout -> listener mail (aviso "tu informe se está generando"). Es la que falla en la demo. */
		informeAviso: 'hotel.reportes.aviso.queue',
		/** Cola del exchange directo -> listener mail que envía el PDF. */
		informeMail: 'hotel.reportes.mail.queue',

		/** "dlq" del diagrama: única DLQ, la consume el listener error. */
		dlq: 'hotel.dlq'
	},

	routingKeys: {
		reservaCreada: 'reserva.creada',
		/** Routing key del exchange directo (flecha "rk" del diagrama). */
		informeGenerado: 'informe.generado',
		/** Todas las colas mandan sus mensajes muertos al DLX con esta routing key. */
		mensajeMuerto: 'mensaje.muerto',
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
