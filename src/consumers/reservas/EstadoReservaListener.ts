import { rabbitConfig } from '../../config/rabbitmq.config';
import { log } from '../../logs/logger';
import { ServicioCorreo } from '../../mail/correo';
import { ErrorNoRecuperable } from '../../messaging/errores';
import { EventoDominio, ReservaEstadoCambiado, validarReservaEstadoCambiado } from '../../messaging/eventos';
import { ContextoMensaje, RabbitListener } from '../../messaging/RabbitListener';

/**
 * Flujo del usuario: avisa al huésped cuando un operador confirma, cancela o completa su reserva.
 * Su cola está enlazada al topic con reserva.estado.* (una sola palabra después de "estado").
 */
export class EstadoReservaListener {
	constructor(private readonly correo: ServicioCorreo) {}

	@RabbitListener({ cola: rabbitConfig.queues.estado, validar: validarReservaEstadoCambiado })
	async avisarCambio(evento: EventoDominio<ReservaEstadoCambiado>, contexto: ContextoMensaje): Promise<void> {
		const reserva = evento.datos;
		const { asunto, texto } = textoCambioEstado(reserva);
		const resultado = await this.correo.enviar({
			destinatario: { nombre: `${reserva.huesped.nombre} ${reserva.huesped.apellido}`, email: reserva.huesped.email },
			asunto,
			texto
		});
		log('info', 'CAMBIO_ESTADO_NOTIFICADO', {
			codigo: reserva.codigo,
			estadoAnterior: reserva.estadoAnterior,
			estado: reserva.estado,
			destinatario: reserva.huesped.email,
			...resultado,
			reintentos: contexto.reintentos
		});
	}
}

export function textoCambioEstado(reserva: ReservaEstadoCambiado): { asunto: string; texto: string } {
	const hola = `Hola ${reserva.huesped.nombre},`;
	switch (reserva.estado) {
		case 'CONFIRMADA':
			return {
				asunto: `Tu reserva ${reserva.codigo} está confirmada`,
				texto: `${hola} confirmamos tu reserva ${reserva.codigo} para la habitación ${reserva.idHabitacion}. ¡Te esperamos!`
			};
		case 'CANCELADA':
			return {
				asunto: `Tu reserva ${reserva.codigo} fue cancelada`,
				texto: `${hola} tu reserva ${reserva.codigo} fue cancelada. Si no lo pediste, responde este correo.`
			};
		case 'COMPLETADA':
			return {
				asunto: `Gracias por hospedarte con nosotros`,
				texto: `${hola} tu estadía de la reserva ${reserva.codigo} terminó. ¡Gracias por elegirnos!`
			};
		default:
			// Un estado que no se notifica es un mensaje mal formado: va directo a la DLQ, sin reintentos.
			throw new ErrorNoRecuperable(`Estado sin aviso al huésped: ${reserva.estado}`);
	}
}
