import { appendFile, mkdir } from 'node:fs/promises';
import { join } from 'node:path';

type Nivel = 'info' | 'warn' | 'error';

const directorioLogs = process.env.LOG_DIR ?? join(process.cwd(), 'logs');

/** Log estructurado (una línea JSON) para seguir los mensajes en `docker compose logs`. */
export function log(nivel: Nivel, evento: string, datos: Record<string, unknown> = {}): void {
	const linea = JSON.stringify({ fecha: new Date().toISOString(), servicio: 'hotel-ms-notificaciones', nivel, evento, ...datos });
	if (nivel === 'error') {
		console.error(linea);
	} else if (nivel === 'warn') {
		console.warn(linea);
	} else {
		console.log(linea);
	}
}

/** Además de la consola, los mensajes muertos quedan en logs/dlq.log para revisarlos después. */
export async function registrarEnArchivoDlq(datos: Record<string, unknown>): Promise<void> {
	await mkdir(directorioLogs, { recursive: true });
	await appendFile(join(directorioLogs, 'dlq.log'), `${JSON.stringify({ fecha: new Date().toISOString(), ...datos })}\n`, 'utf8');
}
