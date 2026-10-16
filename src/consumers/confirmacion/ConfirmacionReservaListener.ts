import { rabbitConfig } from '../../config/rabbitmq.config';
import { log } from '../../logs/logger';
import { EventoDominio, ReservaCreada, validarReservaCreada } from '../../messaging/eventos';
import { PublicadorDocumentos } from '../../messaging/publicadorDocumentos';
import { ContextoMensaje, RabbitListener } from '../../messaging/RabbitListener';
import { comprobanteReserva } from '../../pdf/pdf';

/** Cola 1 · Confirmación de la reserva: genera el comprobante PDF y lo deja listo para enviarse. */
export class ConfirmacionReservaListener {
	constructor(private readonly documentos: PublicadorDocumentos) {}

	@RabbitListener({ cola: rabbitConfig.queues.confirmacion, validar: validarReservaCreada })
	async generarComprobante(evento: EventoDominio<ReservaCreada>, contexto: ContextoMensaje): Promise<void> {
		const reserva = evento.datos;
		const pdf = await comprobanteReserva(reserva);

		await this.documentos.publicar({
			tipoDocumento: 'CONFIRMACION_RESERVA',
			destinatario: { nombre: `${reserva.huesped.nombre} ${reserva.huesped.apellido}`, email: reserva.huesped.email },
			asunto: `Confirmación de tu reserva ${reserva.codigo}`,
			mensaje: `Hola ${reserva.huesped.nombre}, adjuntamos el comprobante de tu reserva ${reserva.codigo}.`,
			nombreArchivo: `reserva-${reserva.codigo.replace(/[^A-Za-z0-9_-]/g, '_')}.pdf`,
			pdfBase64: pdf.toString('base64'),
			referencia: reserva.codigo
		});
		log('info', 'COMPROBANTE_GENERADO', { codigo: reserva.codigo, bytes: pdf.length, reintentos: contexto.reintentos });
	}
}
