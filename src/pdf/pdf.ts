import PDFDocument from 'pdfkit';
import { InformeAdministracion } from '../consumers/reportes/reporteRepository';
import { ReservaCreada } from '../messaging/eventos';

const moneda = new Intl.NumberFormat('es-CL', { style: 'currency', currency: 'CLP' });
const fechaCorta = new Intl.DateTimeFormat('es-CL', { dateStyle: 'long', timeZone: 'UTC' });

function renderizar(titulo: string, dibujar: (doc: PDFKit.PDFDocument) => void): Promise<Buffer> {
	return new Promise((resolve, reject) => {
		const doc = new PDFDocument({ size: 'A4', margin: 50, info: { Title: titulo, Author: 'Hotel Valparaíso' } });
		const partes: Buffer[] = [];
		doc.on('data', (parte: Buffer) => partes.push(parte));
		doc.on('end', () => resolve(Buffer.concat(partes)));
		doc.on('error', reject);

		doc.fontSize(20).font('Helvetica-Bold').text('Hotel Valparaíso');
		doc.fontSize(14).font('Helvetica').fillColor('#555').text(titulo).fillColor('#000');
		doc.moveDown().moveTo(50, doc.y).lineTo(545, doc.y).stroke('#ccc').moveDown();

		dibujar(doc);

		doc.moveDown(2).fontSize(8).fillColor('#888').text(`Documento generado automáticamente el ${new Date().toISOString()}`);
		doc.end();
	});
}

function fila(doc: PDFKit.PDFDocument, etiqueta: string, valor: string): void {
	doc.fontSize(11).font('Helvetica-Bold').text(`${etiqueta}: `, { continued: true }).font('Helvetica').text(valor);
}

function noches(entrada: string, salida: string): number {
	return Math.max(1, Math.round((Date.parse(salida) - Date.parse(entrada)) / 86_400_000));
}

export function comprobanteReserva(reserva: ReservaCreada): Promise<Buffer> {
	return renderizar(`Confirmación de reserva ${reserva.codigo}`, (doc) => {
		doc.fontSize(12).text(`Hola ${reserva.huesped.nombre} ${reserva.huesped.apellido}, tu reserva quedó registrada con estos datos:`);
		doc.moveDown();
		fila(doc, 'Código', reserva.codigo);
		fila(doc, 'Habitación', reserva.idHabitacion);
		fila(doc, 'Entrada', fechaCorta.format(new Date(reserva.fechaEntrada)));
		fila(doc, 'Salida', fechaCorta.format(new Date(reserva.fechaSalida)));
		fila(doc, 'Noches', String(noches(reserva.fechaEntrada, reserva.fechaSalida)));
		fila(doc, 'Huéspedes', String(reserva.cantidadHuespedes));
		fila(doc, 'Precio por noche', moneda.format(reserva.precioPorNoche));
		fila(doc, 'Total', moneda.format(reserva.precioTotal));
		fila(doc, 'Estado', reserva.estado);
		if (reserva.observaciones) {
			fila(doc, 'Observaciones', reserva.observaciones);
		}
		doc.moveDown().fontSize(10).text('Presenta este comprobante al hacer check-in. ¡Te esperamos!');
	});
}

export function informeAdministracion(informe: InformeAdministracion): Promise<Buffer> {
	return renderizar(`Informe de reservas ${informe.desde} a ${informe.hasta}`, (doc) => {
		doc.fontSize(13).font('Helvetica-Bold').text('Resumen');
		fila(doc, 'Reservas', String(informe.totalReservas));
		fila(doc, 'Reservas activas (sin canceladas)', String(informe.reservasActivas));
		fila(doc, 'Ingresos (sin canceladas)', moneda.format(informe.ingresos));
		fila(doc, 'Ticket promedio', moneda.format(informe.ticketPromedio));
		fila(doc, 'Noches vendidas', String(informe.nochesVendidas));
		fila(doc, 'Huéspedes', String(informe.huespedes));

		doc.moveDown().fontSize(13).font('Helvetica-Bold').text('Reservas por estado');
		if (informe.porEstado.length === 0) {
			doc.fontSize(11).font('Helvetica').text('Sin reservas en el periodo.');
		}
		for (const estado of informe.porEstado) {
			fila(doc, estado.estado, `${estado.cantidad} reservas · ${moneda.format(estado.monto)}`);
		}

		doc.moveDown().fontSize(13).font('Helvetica-Bold').text('Habitaciones con más ingresos');
		if (informe.porHabitacion.length === 0) {
			doc.fontSize(11).font('Helvetica').text('Sin reservas en el periodo.');
		}
		for (const habitacion of informe.porHabitacion) {
			fila(doc, habitacion.idHabitacion, `${habitacion.reservas} reservas · ${habitacion.noches} noches · ${moneda.format(habitacion.ingresos)}`);
		}
	});
}
