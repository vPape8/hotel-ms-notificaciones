import { rabbitConfig } from '../../config/rabbitmq.config';
import { log } from '../../logs/logger';
import { PublicadorMensajes } from '../../messaging/ContenedorListeners';
import { crearEvento, EventoDominio, InformeGenerado, ReporteSolicitado, validarReporteSolicitado } from '../../messaging/eventos';
import { ContextoMensaje, RabbitListener } from '../../messaging/RabbitListener';
import { informeAdministracion } from '../../pdf/pdf';
import { ReporteRepository } from './reporteRepository';

/**
 * "listener pdf" del diagrama: consume la cola del fanout, arma el informe en PDF y lo publica
 * en el exchange directo (EX D) con la routing key informe.generado.
 */
export class InformePdfListener {
	constructor(
		private readonly repositorio: ReporteRepository,
		private readonly publicador: PublicadorMensajes
	) {}

	@RabbitListener({ cola: rabbitConfig.queues.informePdf, validar: validarReporteSolicitado })
	async generarPdf(evento: EventoDominio<ReporteSolicitado>, contexto: ContextoMensaje): Promise<void> {
		const { desde, hasta, solicitadoPor } = evento.datos;
		const informe = await this.repositorio.informe(desde, hasta);
		const pdf = await informeAdministracion(informe);

		const generado = crearEvento<InformeGenerado>(rabbitConfig.routingKeys.informeGenerado, {
			destinatario: solicitadoPor,
			asunto: `Informe de reservas ${desde} a ${hasta}`,
			mensaje: `Adjuntamos el informe solicitado: ${informe.totalReservas} reservas en el periodo.`,
			nombreArchivo: `informe-${desde.slice(0, 10)}-a-${hasta.slice(0, 10)}-${evento.id.slice(0, 8)}.pdf`,
			pdfBase64: pdf.toString('base64'),
			solicitudId: evento.id
		});
		await this.publicador.publicar(rabbitConfig.exchanges.reportesDirect, rabbitConfig.routingKeys.informeGenerado, generado, {
			messageId: generado.id,
			type: generado.tipo,
			correlationId: evento.id
		});
		log('info', 'INFORME_PDF_GENERADO', { solicitudId: evento.id, desde, hasta, totalReservas: informe.totalReservas, bytes: pdf.length, reintentos: contexto.reintentos });
	}
}
