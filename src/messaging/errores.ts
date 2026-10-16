/**
 * Error que no se arregla reintentando (mensaje mal formado, datos inválidos, destinatario rechazado).
 * El consumidor hace NACK sin requeue y el mensaje va directo a su DLQ.
 * Cualquier otro error se considera recuperable (BD caída, SMTP no responde) y se reintenta.
 */
export class ErrorNoRecuperable extends Error {
	constructor(mensaje: string) {
		super(mensaje);
		this.name = 'ErrorNoRecuperable';
	}
}
