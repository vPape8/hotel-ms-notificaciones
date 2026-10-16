import 'dotenv/config';
import { createServer } from 'node:http';
import { createDatabasePool } from './config/database';
import { ConfirmacionReservaListener } from './consumers/confirmacion/ConfirmacionReservaListener';
import { DeadLetterListener } from './consumers/dlq/DeadLetterListener';
import { crearCanalesEnvio } from './consumers/notificaciones/canalesEnvio';
import { NotificacionListener } from './consumers/notificaciones/NotificacionListener';
import { MySqlReporteRepository } from './consumers/reportes/reporteRepository';
import { ReporteListener } from './consumers/reportes/ReporteListener';
import { log } from './logs/logger';
import { ContenedorListeners } from './messaging/ContenedorListeners';
import { PublicadorDocumentos } from './messaging/publicadorDocumentos';
import { listenersRegistrados } from './messaging/RabbitListener';

const puerto = Number(process.env.PORT ?? 8084);
const pool = createDatabasePool();
const contenedor = new ContenedorListeners(listenersRegistrados);
const documentos = new PublicadorDocumentos(contenedor);

// Cada instancia registra sus métodos @RabbitListener; el contenedor los suscribe al conectar.
new ConfirmacionReservaListener(documentos);
new ReporteListener(new MySqlReporteRepository(pool), documentos);
new NotificacionListener(crearCanalesEnvio());
new DeadLetterListener();

// /health: 200 solo si hay conexión con RabbitMQ (lo usa el healthcheck de docker-compose).
const servidor = createServer((solicitud, respuesta) => {
	if (solicitud.method === 'GET' && solicitud.url === '/health') {
		const conectado = contenedor.estaConectado;
		respuesta.writeHead(conectado ? 200 : 503, { 'Content-Type': 'application/json' });
		respuesta.end(JSON.stringify({ status: conectado ? 'ok' : 'sin-rabbitmq' }));
		return;
	}
	respuesta.writeHead(404, { 'Content-Type': 'application/json' });
	respuesta.end(JSON.stringify({ error: 'RUTA_NO_ENCONTRADA' }));
});

servidor.listen(puerto, () => {
	log('info', 'SERVICIO_INICIADO', { puerto });
	void contenedor.iniciar();
});

for (const senal of ['SIGINT', 'SIGTERM'] as const) {
	process.once(senal, () => {
		servidor.close();
		void contenedor.detener().then(() => pool.end()).finally(() => process.exit(0));
	});
}
