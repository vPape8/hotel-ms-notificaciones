import assert from 'node:assert/strict';
import test from 'node:test';
import { ErrorNoRecuperable } from '../../messaging/errores';
import { validarEventoReserva, validarReservaEstadoCambiado } from '../../messaging/eventos';
import { textoConfirmacion } from './ConfirmacionReservaListener';
import { textoCambioEstado } from './EstadoReservaListener';

const reserva = {
	codigo: 'RES-1',
	idHabitacion: 'vpo-001-room-1',
	huesped: { nombre: 'Ana', apellido: 'Pérez', email: 'ana@hotel.cl' },
	fechaEntrada: '2026-12-01T15:00:00.000Z',
	fechaSalida: '2026-12-03T11:00:00.000Z',
	cantidadHuespedes: 2,
	estado: 'CONFIRMADA',
	precioPorNoche: 100,
	precioTotal: 200
};

test('cambio de estado: valida el mensaje y arma el correo según el estado nuevo', () => {
	const evento = validarReservaEstadoCambiado({ id: 'e-1', tipo: 'reserva.estado.confirmada', datos: { ...reserva, estadoAnterior: 'PENDIENTE' } });
	assert.equal(evento.datos.estadoAnterior, 'PENDIENTE');
	assert.match(textoCambioEstado(evento.datos).asunto, /confirmada/);
	assert.match(textoCambioEstado({ ...evento.datos, estado: 'CANCELADA' }).asunto, /cancelada/);
	assert.throws(() => textoCambioEstado({ ...evento.datos, estado: 'PENDIENTE' }), ErrorNoRecuperable);
	// Sin estadoAnterior el mensaje está mal formado: va directo a la DLQ.
	assert.throws(() => validarReservaEstadoCambiado({ id: 'e-2', tipo: 'reserva.estado.cancelada', datos: reserva }), ErrorNoRecuperable);
});

test('la cola de registro acepta la creación y los cambios de estado', () => {
	assert.equal(validarEventoReserva({ id: 'e-1', tipo: 'reserva.creada', datos: reserva }).tipo, 'reserva.creada');
	assert.equal(validarEventoReserva({ id: 'e-2', tipo: 'reserva.estado.completada', datos: { ...reserva, estadoAnterior: 'CONFIRMADA' } }).tipo, 'reserva.estado.completada');
	assert.throws(() => validarEventoReserva({ id: 'e-3', tipo: 'reserva.borrada', datos: reserva }), ErrorNoRecuperable);
});

test('el correo de alta no dice "confirmada" si la reserva parte pendiente', () => {
	assert.equal(textoConfirmacion({ ...reserva, estado: 'PENDIENTE' }).asunto, 'Recibimos tu reserva RES-1');
	assert.match(textoConfirmacion({ ...reserva, estado: 'PENDIENTE' }).texto, /pendiente de confirmación/);
	assert.match(textoConfirmacion(reserva).asunto, /confirmada/);
});
