// Decorador @RabbitListener (equivalente al de Spring AMQP).
// Marca un método como consumidor de una cola; ContenedorListeners lo suscribe al conectar y
// se encarga del ACK, NACK, reintentos y DLQ, así los listeners solo tienen lógica de negocio.

export interface ContextoMensaje {
	cola: string;
	routingKey: string;
	messageId?: string;
	/** Veces que el mensaje ya se reintentó (header x-reintentos). */
	reintentos: number;
	headers: Record<string, unknown>;
}

export interface OpcionesListener<T> {
	cola: string;
	/** Convierte el JSON recibido al tipo esperado; lanza ErrorNoRecuperable si no es válido. */
	validar?: (cuerpo: unknown) => T;
	/** false en los consumidores de DLQ: un error ahí no debe volver a reintentarse. */
	reintentar?: boolean;
}

export interface ListenerRegistrado {
	nombre: string;
	cola: string;
	reintentar: boolean;
	validar: (cuerpo: unknown) => unknown;
	manejar: (datos: unknown, contexto: ContextoMensaje) => Promise<void>;
}

const registro: ListenerRegistrado[] = [];

export function RabbitListener<T>(opciones: OpcionesListener<T>) {
	return function <This extends object>(
		metodo: (this: This, datos: T, contexto: ContextoMensaje) => Promise<void>,
		contexto: ClassMethodDecoratorContext<This, (this: This, datos: T, contexto: ContextoMensaje) => Promise<void>>
	): void {
		contexto.addInitializer(function (this: This) {
			registro.push({
				nombre: `${this.constructor.name}.${String(contexto.name)}`,
				cola: opciones.cola,
				reintentar: opciones.reintentar ?? true,
				validar: opciones.validar ?? ((cuerpo) => cuerpo),
				manejar: (datos, ctx) => metodo.call(this, datos as T, ctx)
			});
		});
	};
}

/** Listeners de todas las instancias creadas (cada `new` de una clase con @RabbitListener se registra). */
export function listenersRegistrados(): readonly ListenerRegistrado[] {
	return registro;
}
