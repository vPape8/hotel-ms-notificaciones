import { rabbitConfig } from '../config/rabbitmq.config';
import { PublicadorMensajes } from './ContenedorListeners';
import { crearEvento, DocumentoGenerado } from './eventos';

/** Paso "GEN PDF -> EX" del diagrama: entrega el PDF al exchange de documentos con su routing key. */
export class PublicadorDocumentos {
	constructor(private readonly publicador: PublicadorMensajes) {}

	async publicar(documento: DocumentoGenerado): Promise<void> {
		const evento = crearEvento(rabbitConfig.routingKeys.documentoGenerado, documento);
		await this.publicador.publicar(rabbitConfig.exchanges.documentos, rabbitConfig.routingKeys.documentoGenerado, evento, {
			messageId: evento.id,
			type: evento.tipo,
			correlationId: documento.referencia
		});
	}
}
