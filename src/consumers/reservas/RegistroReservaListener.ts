import { rabbitConfig } from '../../config/rabbitmq.config';
import { log } from '../../logs/logger';
import { EventoDominio, ReservaCreada, ReservaEstadoCambiado, TIPOS_CAMBIO_ESTADO, validarEventoReserva } from '../../messaging/eventos';
import { ContextoMensaje, RabbitListener } from '../../messaging/RabbitListener';
import { ReporteRepository } from '../reportes/reporteRepository';

/**
 * Guarda una copia mínima de cada reserva (tabla reporte_reservas) para el informe PDF del admin.
 * Su cola está enlazada con reserva.#: recibe la creación y cada cambio de estado, así el informe
 * cuenta las reservas confirmadas o canceladas con su estado real.
 */
export class RegistroReservaListener {
	constructor(private readonly repositorio: ReporteRepository) {}

	@RabbitListener({ cola: rabbitConfig.queues.registro, validar: validarEventoReserva })
	async registrar(evento: EventoDominio<ReservaCreada | ReservaEstadoCambiado>, contexto: ContextoMensaje): Promise<void> {
		const esCambioDeEstado = TIPOS_CAMBIO_ESTADO.includes(evento.tipo);
		await this.repositorio.registrarReserva(evento.datos, evento.id, esCambioDeEstado);
		log('info', 'RESERVA_REGISTRADA_EN_INFORMES', {
			codigo: evento.datos.codigo,
			tipo: evento.tipo,
			estado: evento.datos.estado,
			reintentos: contexto.reintentos
		});
	}
}
