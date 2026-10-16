import assert from 'node:assert/strict';
import test from 'node:test';
import { ConsumeMessage } from 'amqplib';
import { rabbitConfig } from '../config/rabbitmq.config';
import { ErrorNoRecuperable } from './errores';
import { validarReservaCreada } from './eventos';
import { OperacionesMensaje, procesarMensaje } from './procesarMensaje';
import { ListenerRegistrado } from './RabbitListener';

function mensaje(cuerpo: unknown, reintentos?: number): ConsumeMessage {
	return {
		content: Buffer.from(typeof cuerpo === 'string' ? cuerpo : JSON.stringify(cuerpo)),
		fields: { deliveryTag: 1, redelivered: false, exchange: 'x', routingKey: 'reserva.creada', consumerTag: 'c' },
		properties: { headers: reintentos === undefined ? {} : { [rabbitConfig.reintentos.header]: reintentos }, messageId: 'm-1' }
	} as unknown as ConsumeMessage;
}

function listener(manejar: ListenerRegistrado['manejar'], reintentar = true): ListenerRegistrado {
	return { nombre: 'Prueba.metodo', cola: 'cola.prueba', reintentar, validar: (cuerpo) => cuerpo, manejar };
}

function operaciones(fallaReintento = false) {
	const registro: string[] = [];
	const ops: OperacionesMensaje = {
		ack: () => registro.push('ack'),
		nack: (_mensaje, requeue) => registro.push(`nack:${requeue}`),
		reintentar: async (_mensaje, cola, reintentos) => {
			if (fallaReintento) throw new Error('broker caído');
			registro.push(`retry:${cola}:${reintentos}`);
		}
	};
	return { ops, registro };
}

test('hace ACK cuando el listener termina bien', async () => {
	const { ops, registro } = operaciones();
	assert.equal(await procesarMensaje(mensaje({ a: 1 }), listener(async () => {}), ops), 'ACK');
	assert.deepEqual(registro, ['ack']);
});

test('error no recuperable: NACK sin requeue (va a la DLQ)', async () => {
	const { ops, registro } = operaciones();
	const resultado = await procesarMensaje(mensaje({}), listener(async () => { throw new ErrorNoRecuperable('dato malo'); }), ops);
	assert.equal(resultado, 'DLQ');
	assert.deepEqual(registro, ['nack:false']);
});

test('JSON inválido va directo a la DLQ', async () => {
	const { ops, registro } = operaciones();
	assert.equal(await procesarMensaje(mensaje('{no-json'), listener(async () => {}), ops), 'DLQ');
	assert.deepEqual(registro, ['nack:false']);
});

test('error recuperable: programa un reintento con el contador incrementado y hace ACK del original', async () => {
	const { ops, registro } = operaciones();
	const resultado = await procesarMensaje(mensaje({}, 1), listener(async () => { throw new Error('BD caída'); }), ops);
	assert.equal(resultado, 'REINTENTO');
	assert.deepEqual(registro, ['retry:cola.prueba:2', 'ack']);
});

test('reintentos agotados: NACK sin requeue', async () => {
	const { ops, registro } = operaciones();
	const resultado = await procesarMensaje(mensaje({}, rabbitConfig.reintentos.maximo), listener(async () => { throw new Error('BD caída'); }), ops);
	assert.equal(resultado, 'DLQ');
	assert.deepEqual(registro, ['nack:false']);
});

test('si no se puede programar el reintento, NACK con requeue para no perder el mensaje', async () => {
	const { ops, registro } = operaciones(true);
	const resultado = await procesarMensaje(mensaje({}), listener(async () => { throw new Error('SMTP caído'); }), ops);
	assert.equal(resultado, 'REQUEUE');
	assert.deepEqual(registro, ['nack:true']);
});

test('los consumidores de DLQ nunca reintentan y aceptan cuerpos que no son JSON', async () => {
	const { ops, registro } = operaciones();
	let recibido: unknown;
	await procesarMensaje(mensaje('texto plano'), listener(async (datos) => { recibido = datos; }, false), ops);
	assert.equal(recibido, 'texto plano');
	assert.deepEqual(registro, ['ack']);
});

test('valida el evento reserva.creada', () => {
	const evento = {
		id: 'e-1', tipo: 'reserva.creada', datos: {
			codigo: 'RES-1', idHabitacion: 'HAB-1', huesped: { nombre: 'Ana', apellido: 'Pérez', email: 'ana@hotel.cl' },
			fechaEntrada: '2026-10-01T15:00:00Z', fechaSalida: '2026-10-03T11:00:00Z', cantidadHuespedes: 2,
			estado: 'PENDIENTE', precioPorNoche: 100, precioTotal: 200
		}
	};
	assert.equal(validarReservaCreada(evento).datos.codigo, 'RES-1');
	assert.throws(() => validarReservaCreada({ ...evento, datos: { ...evento.datos, huesped: { nombre: 'Ana' } } }), ErrorNoRecuperable);
	assert.throws(() => validarReservaCreada({ ...evento, tipo: 'otro' }), ErrorNoRecuperable);
});
