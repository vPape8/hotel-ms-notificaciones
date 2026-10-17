import 'dotenv/config';
import { createServer } from 'node:http';
import { createDatabasePool } from './config/database';
import { ErrorListener } from './consumers/dlq/ErrorListener';
import { AvisoInformeListener } from './consumers/reportes/AvisoInformeListener';
import { InformeMailListener } from './consumers/reportes/InformeMailListener';
import { InformePdfListener } from './consumers/reportes/InformePdfListener';
import { MySqlReporteRepository } from './consumers/reportes/reporteRepository';
import { ConfirmacionReservaListener } from './consumers/reservas/ConfirmacionReservaListener';
import { RegistroReservaListener } from './consumers/reservas/RegistroReservaListener';
import { log } from './logs/logger';
import { crearServicioCorreo } from './mail/correo';
import { ContenedorListeners } from './messaging/ContenedorListeners';
import { listenersRegistrados } from './messaging/RabbitListener';

const puerto = Number(process.env.PORT ?? 8084);
const pool = createDatabasePool();
const repositorio = new MySqlReporteRepository(pool);
const correo = crearServicioCorreo();
const contenedor = new ContenedorListeners(listenersRegistrados);

// Cada instancia registra sus métodos @RabbitListener; el contenedor los suscribe al conectar.
// Usuario: confirmación de la reserva
new ConfirmacionReservaListener(correo);
new RegistroReservaListener(repositorio);
// Admin: informe (fanout -> pdf / aviso, directo -> mail)
new InformePdfListener(repositorio, contenedor);
new AvisoInformeListener(correo);
new InformeMailListener(correo);
// DLQ
new ErrorListener();

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
