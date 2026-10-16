import { randomUUID } from 'node:crypto';
import { ErrorNoRecuperable } from './errores';

// Formato de los mensajes que llegan (de hotel-ms-reservas) y que salen (documento.generado).

export interface EventoDominio<T> {
	id: string;
	tipo: string;
	version: number;
	origen: string;
	ocurridoEn: string;
	datos: T;
}

export interface ReservaCreada {
	codigo: string;
	idHabitacion: string;
	huesped: { nombre: string; apellido: string; email: string; telefono?: string };
	fechaEntrada: string;
	fechaSalida: string;
	cantidadHuespedes: number;
	estado: string;
	precioPorNoche: number;
	precioTotal: number;
	observaciones?: string;
}

export interface ReporteSolicitado {
	desde: string;
	hasta: string;
	solicitadoPor: { nombre?: string; email: string };
}

export type TipoDocumento = 'CONFIRMACION_RESERVA' | 'INFORME_ADMINISTRACION';

export interface DocumentoGenerado {
	tipoDocumento: TipoDocumento;
	destinatario: { nombre?: string; email: string };
	asunto: string;
	mensaje: string;
	nombreArchivo: string;
	/** PDF en base64: el mensaje viaja completo, sin depender de un disco compartido. */
	pdfBase64: string;
	referencia: string;
}

const NOMBRE_ARCHIVO = /^[A-Za-z0-9._-]{1,120}\.pdf$/;

function esObjeto(valor: unknown): valor is Record<string, unknown> {
	return typeof valor === 'object' && valor !== null && !Array.isArray(valor);
}

function texto(objeto: Record<string, unknown>, campo: string): string {
	const valor = objeto[campo];
	if (typeof valor !== 'string' || !valor.trim()) {
		throw new ErrorNoRecuperable(`Campo "${campo}" ausente o vacío`);
	}
	return valor;
}

function numero(objeto: Record<string, unknown>, campo: string): number {
	const valor = objeto[campo];
	if (typeof valor !== 'number' || !Number.isFinite(valor)) {
		throw new ErrorNoRecuperable(`Campo "${campo}" debe ser numérico`);
	}
	return valor;
}

function email(objeto: Record<string, unknown>, campo: string): string {
	const valor = texto(objeto, campo);
	if (!/^[^\s@]+@[^\s@]+$/.test(valor)) {
		throw new ErrorNoRecuperable(`Campo "${campo}" no es un email válido`);
	}
	return valor;
}

function fecha(objeto: Record<string, unknown>, campo: string): string {
	const valor = texto(objeto, campo);
	if (Number.isNaN(Date.parse(valor))) {
		throw new ErrorNoRecuperable(`Campo "${campo}" no es una fecha válida`);
	}
	return valor;
}

function sobre(cuerpo: unknown, tiposAceptados: string[]): EventoDominio<Record<string, unknown>> {
	if (!esObjeto(cuerpo) || !esObjeto(cuerpo.datos)) {
		throw new ErrorNoRecuperable('El mensaje no tiene el formato { id, tipo, datos }');
	}
	const tipo = texto(cuerpo, 'tipo');
	if (!tiposAceptados.includes(tipo)) {
		throw new ErrorNoRecuperable(`Tipo de evento no soportado: ${tipo}`);
	}
	return {
		id: texto(cuerpo, 'id'),
		tipo,
		version: typeof cuerpo.version === 'number' ? cuerpo.version : 1,
		origen: typeof cuerpo.origen === 'string' ? cuerpo.origen : 'desconocido',
		ocurridoEn: typeof cuerpo.ocurridoEn === 'string' ? cuerpo.ocurridoEn : new Date().toISOString(),
		datos: cuerpo.datos
	};
}

function validarDatosReserva(datos: Record<string, unknown>): ReservaCreada {
	const huesped = datos.huesped;
	if (!esObjeto(huesped)) {
		throw new ErrorNoRecuperable('Campo "huesped" ausente');
	}
	return {
		codigo: texto(datos, 'codigo'),
		idHabitacion: texto(datos, 'idHabitacion'),
		huesped: {
			nombre: texto(huesped, 'nombre'),
			apellido: texto(huesped, 'apellido'),
			email: email(huesped, 'email'),
			telefono: typeof huesped.telefono === 'string' ? huesped.telefono : undefined
		},
		fechaEntrada: fecha(datos, 'fechaEntrada'),
		fechaSalida: fecha(datos, 'fechaSalida'),
		cantidadHuespedes: numero(datos, 'cantidadHuespedes'),
		estado: texto(datos, 'estado'),
		precioPorNoche: numero(datos, 'precioPorNoche'),
		precioTotal: numero(datos, 'precioTotal'),
		observaciones: typeof datos.observaciones === 'string' ? datos.observaciones : undefined
	};
}

function validarDatosReporte(datos: Record<string, unknown>): ReporteSolicitado {
	const solicitadoPor = datos.solicitadoPor;
	if (!esObjeto(solicitadoPor)) {
		throw new ErrorNoRecuperable('Campo "solicitadoPor" ausente');
	}
	return {
		desde: fecha(datos, 'desde'),
		hasta: fecha(datos, 'hasta'),
		solicitadoPor: {
			nombre: typeof solicitadoPor.nombre === 'string' ? solicitadoPor.nombre : undefined,
			email: email(solicitadoPor, 'email')
		}
	};
}

export function validarReservaCreada(cuerpo: unknown): EventoDominio<ReservaCreada> {
	const evento = sobre(cuerpo, ['reserva.creada']);
	return { ...evento, datos: validarDatosReserva(evento.datos) };
}

/** La cola de informes recibe los dos tipos de evento. */
export function validarEventoReporte(cuerpo: unknown): EventoDominio<ReservaCreada> | EventoDominio<ReporteSolicitado> {
	const evento = sobre(cuerpo, ['reserva.creada', 'reporte.solicitado']);
	return evento.tipo === 'reserva.creada'
		? { ...evento, datos: validarDatosReserva(evento.datos) }
		: { ...evento, datos: validarDatosReporte(evento.datos) };
}

export function validarDocumentoGenerado(cuerpo: unknown): EventoDominio<DocumentoGenerado> {
	const evento = sobre(cuerpo, ['documento.generado']);
	const datos = evento.datos;
	const destinatario = datos.destinatario;
	if (!esObjeto(destinatario)) {
		throw new ErrorNoRecuperable('Campo "destinatario" ausente');
	}
	const tipoDocumento = texto(datos, 'tipoDocumento');
	if (tipoDocumento !== 'CONFIRMACION_RESERVA' && tipoDocumento !== 'INFORME_ADMINISTRACION') {
		throw new ErrorNoRecuperable(`Tipo de documento no soportado: ${tipoDocumento}`);
	}
	const nombreArchivo = texto(datos, 'nombreArchivo');
	if (!NOMBRE_ARCHIVO.test(nombreArchivo)) {
		throw new ErrorNoRecuperable('nombreArchivo inválido');
	}
	return {
		...evento,
		datos: {
			tipoDocumento,
			destinatario: {
				nombre: typeof destinatario.nombre === 'string' ? destinatario.nombre : undefined,
				email: email(destinatario, 'email')
			},
			asunto: texto(datos, 'asunto'),
			mensaje: texto(datos, 'mensaje'),
			nombreArchivo,
			pdfBase64: texto(datos, 'pdfBase64'),
			referencia: texto(datos, 'referencia')
		}
	};
}

export function crearEvento<T>(tipo: string, datos: T): EventoDominio<T> {
	return { id: randomUUID(), tipo, version: 1, origen: 'hotel-ms-notificaciones', ocurridoEn: new Date().toISOString(), datos };
}
