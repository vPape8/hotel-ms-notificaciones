import { rabbitConfig } from '../../config/rabbitmq.config';
import { log } from '../../logs/logger';
import { ServicioCorreo } from '../../mail/correo';
import { EventoDominio, ReporteSolicitado, validarReporteSolicitado } from '../../messaging/eventos';
import { ContextoMensaje, RabbitListener } from '../../messaging/RabbitListener';

/** Falla simulada: se trata como recuperable, así que se ve el ciclo completo reintentos -> DLX -> DLQ. */
export class FalloSimuladoError extends Error {
	constructor(email: string) {
		super(`Fallo simulado: el servidor de correo no responde para ${email}`);
		this.name = 'FalloSimuladoError';
	}
}

/**
 * "listener mail" de la cola del fanout: avisa al admin que su informe se está generando.
 * Es la cola que falla en la demo: si el email termina en SIMULAR_FALLO_DOMINIO (@falla.test),
 * se reintenta 3 veces y termina en hotel.dlq, donde lo registra el ErrorListener.
 * La cola PDF recibe la misma copia del fanout y sigue su camino normal.
 */
export class AvisoInformeListener {
	constructor(private readonly correo: ServicioCorreo) {}

	@RabbitListener({ cola: rabbitConfig.queues.informeAviso, validar: validarReporteSolicitado })
	async enviarAviso(evento: EventoDominio<ReporteSolicitado>, contexto: ContextoMensaje): Promise<void> {
		const { desde, hasta, solicitadoPor } = evento.datos;
		if (solicitadoPor.email.toLowerCase().endsWith(rabbitConfig.falloSimulado.dominio)) {
			throw new FalloSimuladoError(solicitadoPor.email);
		}

		const resultado = await this.correo.enviar({
			destinatario: solicitadoPor,
			asunto: `Estamos generando tu informe ${desde} a ${hasta}`,
			texto: 'Recibimos tu solicitud de informe. Te llegará en otro correo con el PDF adjunto en unos momentos.'
		});
		log('info', 'AVISO_INFORME_ENVIADO', { solicitudId: evento.id, destinatario: solicitadoPor.email, ...resultado, reintentos: contexto.reintentos });
	}
}
