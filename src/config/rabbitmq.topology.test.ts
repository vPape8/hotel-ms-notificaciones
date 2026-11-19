import assert from 'node:assert/strict';
import test from 'node:test';
import { colaDlq, colaReintento, colasDeTrabajo, rabbitConfig } from './rabbitmq.config';
import { colasDlq, topologiaNotificaciones } from './rabbitmq.topology';

const { exchanges, queues } = rabbitConfig;

test('cada cola de trabajo manda sus mensajes muertos a su propia DLQ', () => {
	for (const cola of colasDeTrabajo) {
		const definicion = topologiaNotificaciones.colas.find((actual) => actual.nombre === cola);
		assert.deepEqual(definicion?.argumentos, { 'x-dead-letter-exchange': exchanges.deadLetter, 'x-dead-letter-routing-key': cola });
		assert.ok(topologiaNotificaciones.colas.some((actual) => actual.nombre === colaDlq(cola)), `falta ${colaDlq(cola)}`);
		assert.ok(topologiaNotificaciones.colas.some((actual) => actual.nombre === colaReintento(cola)), `falta ${colaReintento(cola)}`);
		assert.ok(topologiaNotificaciones.bindings.some((binding) =>
			binding.exchange === exchanges.deadLetter && binding.cola === colaDlq(cola) && binding.routingKey === cola));
	}
	// El listener error lee todas las DLQ, más la de mensajes que el productor no pudo entregar.
	assert.equal(colasDlq.length, colasDeTrabajo.length + 1);
	assert.ok(colasDlq.includes(queues.noEntregados));
});

test('el topic de reservas reparte por patrón: creada, estado.* y reserva.#', () => {
	const rutas = topologiaNotificaciones.bindings
		.filter((binding) => binding.exchange === exchanges.reservas)
		.map((binding) => `${binding.routingKey} -> ${binding.cola}`);
	assert.deepEqual(rutas, [
		`reserva.creada -> ${queues.confirmacion}`,
		`reserva.estado.* -> ${queues.estado}`,
		`reserva.# -> ${queues.registro}`
	]);
});
