import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import nodemailer, { Transporter } from 'nodemailer';
import { log } from '../logs/logger';
import { ErrorNoRecuperable } from '../messaging/errores';

export interface MensajeCorreo {
	destinatario: { nombre?: string; email: string };
	asunto: string;
	texto: string;
	adjunto?: { nombreArchivo: string; pdf: Buffer };
}

/** Lo que usan los listeners mail: no saben si hay SMTP real, Mailpit o solo archivo. */
export interface ServicioCorreo {
	enviar(mensaje: MensajeCorreo): Promise<{ messageId?: string; archivo?: string }>;
}

/**
 * Envía por SMTP (en docker-compose, Mailpit: los correos se ven en http://localhost:8025)
 * y guarda una copia de cada PDF en DOCUMENTOS_DIR. Sin SMTP_HOST solo guarda el PDF y deja registro.
 */
export class ServicioCorreoSmtp implements ServicioCorreo {
	private readonly transporte?: Transporter;

	constructor(
		private readonly directorioDocumentos: string,
		private readonly remitente: string,
		smtp?: { host: string; port: number; user?: string; pass?: string; secure: boolean }
	) {
		if (smtp) {
			this.transporte = nodemailer.createTransport({
				host: smtp.host,
				port: smtp.port,
				secure: smtp.secure,
				auth: smtp.user ? { user: smtp.user, pass: smtp.pass } : undefined
			});
		}
	}

	async enviar(mensaje: MensajeCorreo): Promise<{ messageId?: string; archivo?: string }> {
		let archivo: string | undefined;
		if (mensaje.adjunto) {
			await mkdir(this.directorioDocumentos, { recursive: true });
			archivo = join(this.directorioDocumentos, mensaje.adjunto.nombreArchivo);
			await writeFile(archivo, mensaje.adjunto.pdf);
		}

		if (!this.transporte) {
			log('warn', 'CORREO_SIN_SMTP', { destinatario: mensaje.destinatario.email, asunto: mensaje.asunto, archivo });
			return { archivo };
		}

		try {
			const resultado = await this.transporte.sendMail({
				from: this.remitente,
				to: mensaje.destinatario.nombre ? `"${mensaje.destinatario.nombre}" <${mensaje.destinatario.email}>` : mensaje.destinatario.email,
				subject: mensaje.asunto,
				text: mensaje.texto,
				attachments: mensaje.adjunto
					? [{ filename: mensaje.adjunto.nombreArchivo, content: mensaje.adjunto.pdf, contentType: 'application/pdf' }]
					: undefined
			});
			return { messageId: String(resultado.messageId), archivo };
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

export function crearServicioCorreo(): ServicioCorreo {
	return new ServicioCorreoSmtp(
		process.env.DOCUMENTOS_DIR ?? join(process.cwd(), 'documentos'),
		process.env.SMTP_FROM ?? 'Hotel Valparaíso <no-reply@hotel.local>',
		process.env.SMTP_HOST
			? {
				host: process.env.SMTP_HOST,
				port: Number(process.env.SMTP_PORT ?? 587),
				user: process.env.SMTP_USER || undefined,
				pass: process.env.SMTP_PASSWORD || undefined,
				secure: process.env.SMTP_SECURE === 'true'
			}
			: undefined
	);
}
