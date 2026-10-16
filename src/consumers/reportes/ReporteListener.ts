import { rabbitConfig } from '../../config/rabbitmq.config';
import { log } from '../../logs/logger';
import { EventoDominio, ReporteSolicitado, ReservaCreada, validarEventoReporte } from '../../messaging/eventos';
import { PublicadorDocumentos } from '../../messaging/publicadorDocumentos';
import { ContextoMensaje, RabbitListener } from '../../messaging/RabbitListener';
import { informeAdministracion } from '../../pdf/pdf';
import { ReporteRepository } from './reporteRepository';

type EventoReporte = EventoDominio<ReservaCreada> | EventoDominio<ReporteSolicitado>;

function esSolicitud(evento: EventoReporte): evento is EventoDominio<ReporteSolicitado> {
	return evento.tipo === rabbitConfig.routingKeys.reporteSolicitado;
}

/** Cola 2 · Informes para administración. */
export class ReporteListener {
	constructor(
		private readonly repositorio: ReporteRepository,
		private readonly documentos: PublicadorDocumentos
	) {}

	@RabbitListener({ cola: rabbitConfig.queues.reportes, validar: validarEventoReporte })
	async procesar(evento: EventoReporte, contexto: ContextoMensaje): Promise<void> {
		if (esSolicitud(evento)) {
			await this.generarInforme(evento);
			return;
		}
		// reserva.creada: se guarda una copia mínima para poder calcular los informes.
		await this.repositorio.registrarReserva(evento.datos, evento.id);
		log('info', 'RESERVA_REGISTRADA_EN_INFORMES', { codigo: evento.datos.codigo, reintentos: contexto.reintentos });
	}

	private async generarInforme(evento: EventoDominio<ReporteSolicitado>): Promise<void> {
		const { desde, hasta, solicitadoPor } = evento.datos;
		const informe = await this.repositorio.informe(desde, hasta);
		const pdf = await informeAdministracion(informe);

		await this.documentos.publicar({
			tipoDocumento: 'INFORME_ADMINISTRACION',
			destinatario: solicitadoPor,
			asunto: `Informe de reservas ${desde} a ${hasta}`,
			mensaje: `Adjuntamos el informe solicitado: ${informe.totalReservas} reservas y ingresos por ${informe.ingresos}.`,
			nombreArchivo: `informe-${desde.slice(0, 10)}-a-${hasta.slice(0, 10)}-${evento.id.slice(0, 8)}.pdf`,
			pdfBase64: pdf.toString('base64'),
			referencia: evento.id
		});
		log('info', 'INFORME_GENERADO', { id: evento.id, desde, hasta, totalReservas: informe.totalReservas });
	}
}
