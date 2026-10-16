import { rabbitConfig } from '../../config/rabbitmq.config';
import { log } from '../../logs/logger';
import { ErrorNoRecuperable } from '../../messaging/errores';
import { DocumentoGenerado, EventoDominio, validarDocumentoGenerado } from '../../messaging/eventos';
import { ContextoMensaje, RabbitListener } from '../../messaging/RabbitListener';
import { CanalEnvio } from './canalesEnvio';

/** Último paso de las dos colas ("Q -> Consumer" del diagrama): entrega el PDF al destinatario. */
export class NotificacionListener {
	constructor(private readonly canales: CanalEnvio[]) {}

	@RabbitListener({ cola: rabbitConfig.queues.notificaciones, validar: validarDocumentoGenerado })
	async enviarDocumento(evento: EventoDominio<DocumentoGenerado>, contexto: ContextoMensaje): Promise<void> {
		const documento = evento.datos;
		const pdf = Buffer.from(documento.pdfBase64, 'base64');
		if (pdf.subarray(0, 5).toString('latin1') !== '%PDF-') {
			throw new ErrorNoRecuperable('El adjunto no es un PDF');
		}

		const entregas: Record<string, string> = {};
		for (const canal of this.canales) {
			entregas[canal.nombre] = await canal.enviar(documento, pdf);
		}
		log('info', 'DOCUMENTO_ENVIADO', {
			tipoDocumento: documento.tipoDocumento,
			referencia: documento.referencia,
			destinatario: documento.destinatario.email,
			entregas,
			reintentos: contexto.reintentos
		});
	}
}
