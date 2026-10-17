import assert from 'node:assert/strict';
import test from 'node:test';
import { ConsumeMessage } from 'amqplib';
import { rabbitConfig } from '../../config/rabbitmq.config';
import { MensajeCorreo, ServicioCorreo } from '../../mail/correo';
import { OperacionesMensaje, procesarMensaje } from '../../messaging/procesarMensaje';
import { listenersRegistrados } from '../../messaging/RabbitListener';
import { AvisoInformeListener } from './AvisoInformeListener';

class CorreoFalso implements ServicioCorreo {
	enviados: MensajeCorreo[] = [];
	async enviar(mensaje: MensajeCorreo) { this.enviados.push(mensaje); return {}; }
}

function solicitud(email: string, reintentos: number): ConsumeMessage {
	const evento = { id: 'e-1', tipo: 'reporte.solicitado', datos: { desde: '2026-10-01', hasta: '2026-10-31', solicitadoPor: { email } } };
	return {
		content: Buffer.from(JSON.stringify(evento)),
		fields: { deliveryTag: 1, redelivered: false, exchange: rabbitConfig.exchanges.reportesFanout, routingKey: '', consumerTag: 'c' },
		properties: { headers: { [rabbitConfig.reintentos.header]: reintentos }, messageId: 'm-1' }
	} as unknown as ConsumeMessage;
}

test('la cola de aviso falla con @falla.test: reintenta y al agotar los reintentos va a la DLQ', async () => {
	const correo = new CorreoFalso();
	new AvisoInformeListener(correo);
	const listener = listenersRegistrados().find((registrado) => registrado.cola === rabbitConfig.queues.informeAviso);
	assert.ok(listener);

	const registro: string[] = [];
	const ops: OperacionesMensaje = {
		ack: () => registro.push('ack'),
		nack: (_mensaje, requeue) => registro.push(`nack:${requeue}`),
		reintentar: async (_mensaje, _cola, n) => { registro.push(`retry:${n}`); }
	};

	assert.equal(await procesarMensaje(solicitud('admin@hotel.cl', 0), listener, ops), 'ACK');
	assert.equal(correo.enviados.length, 1);

	assert.equal(await procesarMensaje(solicitud('admin@falla.test', 0), listener, ops), 'REINTENTO');
	assert.equal(await procesarMensaje(solicitud('admin@falla.test', rabbitConfig.reintentos.maximo), listener, ops), 'DLQ');
	assert.deepEqual(registro, ['ack', 'retry:1', 'ack', 'nack:false']);
	assert.equal(correo.enviados.length, 1);
});
