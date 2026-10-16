import { RowDataPacket } from 'mysql2';
import { Pool } from 'mysql2/promise';
import { ReservaCreada } from '../../messaging/eventos';

export interface InformeAdministracion {
	desde: string;
	hasta: string;
	totalReservas: number;
	reservasActivas: number;
	ingresos: number;
	ticketPromedio: number;
	nochesVendidas: number;
	huespedes: number;
	porEstado: { estado: string; cantidad: number; monto: number }[];
	porHabitacion: { idHabitacion: string; reservas: number; noches: number; ingresos: number }[];
}

export interface ReporteRepository {
	/** Idempotente: si el mensaje se entrega dos veces, la reserva se guarda una sola vez. */
	registrarReserva(reserva: ReservaCreada, eventoId: string): Promise<void>;
	informe(desde: string, hasta: string): Promise<InformeAdministracion>;
}

const ESQUEMA = `
CREATE TABLE IF NOT EXISTS reporte_reservas (
	codigo VARCHAR(64) PRIMARY KEY,
	evento_id CHAR(36) NOT NULL,
	id_habitacion VARCHAR(64) NOT NULL,
	email_huesped VARCHAR(255) NOT NULL,
	fecha_entrada DATETIME(3) NOT NULL,
	fecha_salida DATETIME(3) NOT NULL,
	noches INT NOT NULL,
	cantidad_huespedes INT NOT NULL,
	estado VARCHAR(20) NOT NULL,
	precio_total DECIMAL(12, 2) NOT NULL,
	registrada_en DATETIME(3) NOT NULL,
	INDEX idx_reporte_fecha_entrada (fecha_entrada)
)`;

interface FilaResumen extends RowDataPacket {
	total: number;
	activas: number | null;
	ingresos: string | null;
	noches: string | null;
	huespedes: string | null;
}

interface FilaEstado extends RowDataPacket {
	estado: string;
	cantidad: number;
	monto: string;
}

interface FilaHabitacion extends RowDataPacket {
	id_habitacion: string;
	reservas: number;
	noches: string;
	ingresos: string;
}

function noches(entrada: string, salida: string): number {
	return Math.max(1, Math.round((Date.parse(salida) - Date.parse(entrada)) / 86_400_000));
}

export class MySqlReporteRepository implements ReporteRepository {
	private esquemaListo?: Promise<void>;

	constructor(private readonly pool: Pool) {}

	private asegurarEsquema(): Promise<void> {
		this.esquemaListo ??= this.pool.query(ESQUEMA).then(() => undefined).catch((error: unknown) => {
			this.esquemaListo = undefined;
			throw error;
		});
		return this.esquemaListo;
	}

	async registrarReserva(reserva: ReservaCreada, eventoId: string): Promise<void> {
		await this.asegurarEsquema();
		await this.pool.execute(
			`INSERT INTO reporte_reservas
				(codigo, evento_id, id_habitacion, email_huesped, fecha_entrada, fecha_salida, noches, cantidad_huespedes, estado, precio_total, registrada_en)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE estado = VALUES(estado), precio_total = VALUES(precio_total)`,
			[
				reserva.codigo, eventoId, reserva.idHabitacion, reserva.huesped.email,
				new Date(reserva.fechaEntrada), new Date(reserva.fechaSalida), noches(reserva.fechaEntrada, reserva.fechaSalida),
				reserva.cantidadHuespedes, reserva.estado, reserva.precioTotal, new Date()
			]
		);
	}

	async informe(desde: string, hasta: string): Promise<InformeAdministracion> {
		await this.asegurarEsquema();
		const rango = [`${desde.slice(0, 10)} 00:00:00`, `${hasta.slice(0, 10)} 23:59:59.999`];

		const [[resumen]] = await this.pool.query<FilaResumen[]>(
			`SELECT COUNT(*) AS total,
				SUM(estado <> 'CANCELADA') AS activas,
				SUM(CASE WHEN estado <> 'CANCELADA' THEN precio_total ELSE 0 END) AS ingresos,
				SUM(CASE WHEN estado <> 'CANCELADA' THEN noches ELSE 0 END) AS noches,
				SUM(CASE WHEN estado <> 'CANCELADA' THEN cantidad_huespedes ELSE 0 END) AS huespedes
			FROM reporte_reservas WHERE fecha_entrada BETWEEN ? AND ?`, rango);

		const [porEstado] = await this.pool.query<FilaEstado[]>(
			`SELECT estado, COUNT(*) AS cantidad, SUM(precio_total) AS monto
			FROM reporte_reservas WHERE fecha_entrada BETWEEN ? AND ?
			GROUP BY estado ORDER BY cantidad DESC`, rango);

		const [porHabitacion] = await this.pool.query<FilaHabitacion[]>(
			`SELECT id_habitacion, COUNT(*) AS reservas, SUM(noches) AS noches, SUM(precio_total) AS ingresos
			FROM reporte_reservas WHERE fecha_entrada BETWEEN ? AND ? AND estado <> 'CANCELADA'
			GROUP BY id_habitacion ORDER BY ingresos DESC LIMIT 10`, rango);

		const activas = Number(resumen?.activas ?? 0);
		const ingresos = Number(resumen?.ingresos ?? 0);
		return {
			desde,
			hasta,
			totalReservas: Number(resumen?.total ?? 0),
			reservasActivas: activas,
			ingresos,
			ticketPromedio: activas > 0 ? Math.round(ingresos / activas) : 0,
			nochesVendidas: Number(resumen?.noches ?? 0),
			huespedes: Number(resumen?.huespedes ?? 0),
			porEstado: porEstado.map((fila) => ({ estado: fila.estado, cantidad: Number(fila.cantidad), monto: Number(fila.monto) })),
			porHabitacion: porHabitacion.map((fila) => ({
				idHabitacion: fila.id_habitacion,
				reservas: Number(fila.reservas),
				noches: Number(fila.noches),
				ingresos: Number(fila.ingresos)
			}))
		};
	}
}
