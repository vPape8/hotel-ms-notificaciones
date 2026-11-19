import { rabbitConfig } from '../../config/rabbitmq.config';
import { log } from '../../logs/logger';
import { ServicioCorreo } from '../../mail/correo';
import { EventoDominio, ReservaCreada, validarReservaCreada } from '../../messaging/eventos';
import { ContextoMensaje, RabbitListener } from '../../messaging/RabbitListener';
import { comprobanteReserva } from '../../pdf/pdf';

/**
 * Flujo del usuario: le avisa al huésped por correo que su reserva quedó registrada, con el comprobante.
 * Una reserva de la web parte PENDIENTE (la confirma un operador); el panel puede crearla ya CONFIRMADA.
 */
export class ConfirmacionReservaListener {
	constructor(private readonly correo: ServicioCorreo) {}

	@RabbitListener({ cola: rabbitConfig.queues.confirmacion, validar: validarReservaCreada })
	async enviarConfirmacion(evento: EventoDominio<ReservaCreada>, contexto: ContextoMensaje): Promise<void> {
		const reserva = evento.datos;
		const pdf = await comprobanteReserva(reserva);

		const resultado = await this.correo.enviar({
			destinatario: { nombre: `${reserva.huesped.nombre} ${reserva.huesped.apellido}`, email: reserva.huesped.email },
			asunto: textoConfirmacion(reserva).asunto,
			texto: textoConfirmacion(reserva).texto,
			adjunto: { nombreArchivo: `reserva-${reserva.codigo.replace(/[^A-Za-z0-9_-]/g, '_')}.pdf`, pdf }
		});
		log('info', 'CONFIRMACION_ENVIADA', { codigo: reserva.codigo, destinatario: reserva.huesped.email, ...resultado, reintentos: contexto.reintentos });
	}
}

export function textoConfirmacion(reserva: ReservaCreada): { asunto: string; texto: string } {
	const saludo = `Hola ${reserva.huesped.nombre}, recibimos tu reserva ${reserva.codigo} para la habitación ${reserva.idHabitacion}.`;
	if (reserva.estado === 'CONFIRMADA') {
		return { asunto: `Tu reserva ${reserva.codigo} está confirmada`, texto: `${saludo} Ya está confirmada y adjuntamos el comprobante. ¡Te esperamos!` };
	}
	return {
		asunto: `Recibimos tu reserva ${reserva.codigo}`,
		texto: `${saludo} Está pendiente de confirmación: te escribiremos apenas la confirmemos. Adjuntamos el comprobante.`
	};
}
