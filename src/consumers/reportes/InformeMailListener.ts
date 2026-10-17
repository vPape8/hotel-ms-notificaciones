import { rabbitConfig } from '../../config/rabbitmq.config';
import { log } from '../../logs/logger';
import { ServicioCorreo } from '../../mail/correo';
import { ErrorNoRecuperable } from '../../messaging/errores';
import { EventoDominio, InformeGenerado, validarInformeGenerado } from '../../messaging/eventos';
import { ContextoMensaje, RabbitListener } from '../../messaging/RabbitListener';

/** "listener mail" de la cola del exchange directo: envía el PDF del informe al admin. */
export class InformeMailListener {
	constructor(private readonly correo: ServicioCorreo) {}

	@RabbitListener({ cola: rabbitConfig.queues.informeMail, validar: validarInformeGenerado })
	async enviarInforme(evento: EventoDominio<InformeGenerado>, contexto: ContextoMensaje): Promise<void> {
		const informe = evento.datos;
		const pdf = Buffer.from(informe.pdfBase64, 'base64');
		if (pdf.subarray(0, 5).toString('latin1') !== '%PDF-') {
			throw new ErrorNoRecuperable('El adjunto no es un PDF');
		}

		const resultado = await this.correo.enviar({
			destinatario: informe.destinatario,
			asunto: informe.asunto,
			texto: informe.mensaje,
			adjunto: { nombreArchivo: informe.nombreArchivo, pdf }
		});
		log('info', 'INFORME_ENVIADO', { solicitudId: informe.solicitudId, destinatario: informe.destinatario.email, ...resultado, reintentos: contexto.reintentos });
	}
}
