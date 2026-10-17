import { rabbitConfig } from '../../config/rabbitmq.config';
import { log } from '../../logs/logger';
import { ServicioCorreo } from '../../mail/correo';
import { EventoDominio, ReservaCreada, validarReservaCreada } from '../../messaging/eventos';
import { ContextoMensaje, RabbitListener } from '../../messaging/RabbitListener';
import { comprobanteReserva } from '../../pdf/pdf';

/** Flujo del usuario: le confirma al huésped por correo que su reserva quedó registrada. */
export class ConfirmacionReservaListener {
	constructor(private readonly correo: ServicioCorreo) {}

	@RabbitListener({ cola: rabbitConfig.queues.confirmacion, validar: validarReservaCreada })
	async enviarConfirmacion(evento: EventoDominio<ReservaCreada>, contexto: ContextoMensaje): Promise<void> {
		const reserva = evento.datos;
		const pdf = await comprobanteReserva(reserva);

		const resultado = await this.correo.enviar({
			destinatario: { nombre: `${reserva.huesped.nombre} ${reserva.huesped.apellido}`, email: reserva.huesped.email },
			asunto: `Tu reserva ${reserva.codigo} está confirmada`,
			texto: `Hola ${reserva.huesped.nombre}, recibimos tu reserva ${reserva.codigo} para la habitación ${reserva.idHabitacion}. `
				+ 'Adjuntamos el comprobante. ¡Te esperamos!',
			adjunto: { nombreArchivo: `reserva-${reserva.codigo.replace(/[^A-Za-z0-9_-]/g, '_')}.pdf`, pdf }
		});
		log('info', 'CONFIRMACION_ENVIADA', { codigo: reserva.codigo, destinatario: reserva.huesped.email, ...resultado, reintentos: contexto.reintentos });
	}
}
