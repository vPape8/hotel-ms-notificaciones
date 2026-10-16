import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import nodemailer, { Transporter } from 'nodemailer';
import { ErrorNoRecuperable } from '../../messaging/errores';
import { DocumentoGenerado } from '../../messaging/eventos';

export interface CanalEnvio {
	readonly nombre: string;
	enviar(documento: DocumentoGenerado, pdf: Buffer): Promise<string>;
}

/** Guarda cada PDF en disco (DOCUMENTOS_DIR): sirve de respaldo y para revisar la demo sin correo. */
export class AlmacenDocumentos implements CanalEnvio {
	readonly nombre = 'archivo';

	constructor(private readonly directorio: string) {}

	async enviar(documento: DocumentoGenerado, pdf: Buffer): Promise<string> {
		await mkdir(this.directorio, { recursive: true });
		const ruta = join(this.directorio, documento.nombreArchivo);
		await writeFile(ruta, pdf);
		return ruta;
	}
}

/** Envía el PDF adjunto por SMTP (en docker-compose apunta a Mailpit, que muestra los correos en :8025). */
export class EnviadorCorreo implements CanalEnvio {
	readonly nombre = 'correo';
	private readonly transporte: Transporter;

	constructor(opciones: { host: string; port: number; user?: string; pass?: string; secure: boolean }, private readonly remitente: string) {
		this.transporte = nodemailer.createTransport({
			host: opciones.host,
			port: opciones.port,
			secure: opciones.secure,
			auth: opciones.user ? { user: opciones.user, pass: opciones.pass } : undefined
		});
	}

	async enviar(documento: DocumentoGenerado, pdf: Buffer): Promise<string> {
		try {
			const resultado = await this.transporte.sendMail({
				from: this.remitente,
				to: documento.destinatario.nombre ? `"${documento.destinatario.nombre}" <${documento.destinatario.email}>` : documento.destinatario.email,
				subject: documento.asunto,
				text: documento.mensaje,
				attachments: [{ filename: documento.nombreArchivo, content: pdf, contentType: 'application/pdf' }]
			});
			return String(resultado.messageId);
		} catch (error) {
			// 5xx del servidor SMTP = rechazo definitivo (destinatario inexistente, etc.): no tiene sentido reintentar.
			const codigo = (error as { responseCode?: number }).responseCode;
			if (typeof codigo === 'number' && codigo >= 500) {
				throw new ErrorNoRecuperable(`SMTP rechazó el correo (${codigo})`);
			}
			throw error;
		}
	}
}

export function crearCanalesEnvio(): CanalEnvio[] {
	const canales: CanalEnvio[] = [new AlmacenDocumentos(process.env.DOCUMENTOS_DIR ?? join(process.cwd(), 'documentos'))];
	if (process.env.SMTP_HOST) {
		canales.push(new EnviadorCorreo({
			host: process.env.SMTP_HOST,
			port: Number(process.env.SMTP_PORT ?? 587),
			user: process.env.SMTP_USER || undefined,
			pass: process.env.SMTP_PASSWORD || undefined,
			secure: process.env.SMTP_SECURE === 'true'
		}, process.env.SMTP_FROM ?? 'Hotel Valparaíso <no-reply@hotel.local>'));
	}
	return canales;
}
