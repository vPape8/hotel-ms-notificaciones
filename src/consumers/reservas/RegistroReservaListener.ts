import { rabbitConfig } from '../../config/rabbitmq.config';
import { log } from '../../logs/logger';
import { EventoDominio, ReservaCreada, validarReservaCreada } from '../../messaging/eventos';
import { ContextoMensaje, RabbitListener } from '../../messaging/RabbitListener';
import { ReporteRepository } from '../reportes/reporteRepository';

/**
 * Guarda una copia mínima de cada reserva (tabla reporte_reservas) para que el informe del admin
 * tenga datos: hotel-ms-reservas todavía no persiste las reservas.
 */
export class RegistroReservaListener {
	constructor(private readonly repositorio: ReporteRepository) {}

	@RabbitListener({ cola: rabbitConfig.queues.registro, validar: validarReservaCreada })
	async registrar(evento: EventoDominio<ReservaCreada>, contexto: ContextoMensaje): Promise<void> {
		await this.repositorio.registrarReserva(evento.datos, evento.id);
		log('info', 'RESERVA_REGISTRADA_EN_INFORMES', { codigo: evento.datos.codigo, reintentos: contexto.reintentos });
	}
}
